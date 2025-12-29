"""
Parquet Writer for Connect4 Move Logging.
Handles writing move events to parquet files and creating ML datasets.
"""
import json
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any
import uuid

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import numpy as np  # ← ADDED: Import numpy

logger = logging.getLogger("logger")


def parse_board_to_matrix(board_str: str | list) -> List[List[int]]:
    """
    Parse board string/list to integer matrix.

    Handles multiple formats:
    - JSON string: '[[".", "X"], ["O", "."]]'
    - Python list: [[".", "X"], ["O", "."]]
    - Numpy array: np.array([[".", "X"], ["O", "."]])  # ← ADDED!

    Returns:
        6x7 integer matrix where 0=empty, 1=player1, 2=player2
    """
    # ✅ FIX #1: Handle numpy arrays (from pandas when reading parquet)
    if isinstance(board_str, np.ndarray):
        board_str = board_str.tolist()  # Convert to Python list

    # Handle list (from RabbitMQ events or after numpy conversion)
    if isinstance(board_str, list):
        # Convert string tokens to integers
        matrix = []
        for row in board_str:
            int_row = []
            for cell in row:
                if cell == '.' or cell == 0 or cell == '0':
                    int_row.append(0)
                elif cell == 'X' or cell == '1' or cell == 1:
                    int_row.append(1)
                elif cell == 'O' or cell == '2' or cell == 2:
                    int_row.append(2)
                else:
                    int_row.append(0)
            matrix.append(int_row)

        # Ensure 6x7 dimensions
        while len(matrix) < 6:
            matrix.insert(0, [0] * 7)
        matrix = matrix[:6]

        for row in matrix:
            while len(row) < 7:
                row.append(0)
            row[:] = row[:7]

        return matrix

    # Handle JSON string
    if isinstance(board_str, str):
        try:
            board_list = json.loads(board_str)
            return parse_board_to_matrix(board_list)
        except json.JSONDecodeError:
            logger.error(f"Failed to parse board string: {board_str}")
            return [[0] * 7 for _ in range(6)]

    # Fallback
    logger.warning(f"Unknown board format: {type(board_str)}")
    return [[0] * 7 for _ in range(6)]


def flatten_board_for_ml(board, prefix='board'):
    """
    Flatten 2D board to 1D dict for ML.
    Handles both live boards (from events) and stored boards (from parquet).
    """
    # ✅ FIX #2: First convert to integer matrix
    int_board = parse_board_to_matrix(board)

    flattened = {}
    for row_idx, row in enumerate(int_board):
        for col_idx, value in enumerate(row):
            flattened[f'{prefix}_r{row_idx}c{col_idx}'] = int(value)
    return flattened


def extract_mcts_features(mcts_stats: Dict) -> Dict[str, Any]:
    """Extract MCTS statistics for ML features."""
    if not mcts_stats:
        return {}

    return {
        'num_rollouts': mcts_stats.get('num_rollouts', 0),
        'num_visits': mcts_stats.get('num_visits', 0),
        'time_budget_ms': mcts_stats.get('time_budget_ms', 0),
        'actual_time_ms': mcts_stats.get('actual_time_ms', 0),
        'nodes_visited': mcts_stats.get('nodes_visited', 0),
        'max_depth': mcts_stats.get('max_depth', 0),
        'avg_depth': mcts_stats.get('avg_depth', 0.0),
        'exploration_constant': mcts_stats.get('exploration_constant', 1.414),
        'tree_size': mcts_stats.get('tree_size', 0),
        'cache_hits': mcts_stats.get('cache_hits', 0),
        'cache_misses': mcts_stats.get('cache_misses', 0),
        'cache_hit_rate': mcts_stats.get('cache_hit_rate', 0.0),
    }


def extract_mcts_policy(mcts_stats: Dict) -> Dict[str, float]:
    """Extract MCTS policy (visit counts distribution)."""
    if not mcts_stats:
        return {}

    visit_counts = mcts_stats.get('visit_counts', {})

    # Convert to policy_col_X format
    policy = {}
    for col, count in visit_counts.items():
        policy[f'policy_col_{col}'] = float(count)

    return policy


def extract_mcts_q_values(mcts_stats: Dict) -> Dict[str, float]:
    """Extract MCTS Q-values for each action."""
    if not mcts_stats:
        return {}

    q_values = mcts_stats.get('q_values', {})

    # Convert to q_value_col_X format
    q_dict = {}
    for col, q_val in q_values.items():
        q_dict[f'q_value_col_{col}'] = float(q_val)

    return q_dict


def extract_reference_features(reference_stats: Dict) -> Dict[str, Any]:
    """Extract reference agent statistics."""
    if not reference_stats:
        return {
            'reference_move': None,
            'reference_rollouts': 0,
            'reference_visits': 0,
            'reference_time_ms': 0,
            'reference_nodes': 0,
            'reference_max_depth': 0,
            'reference_avg_depth': 0.0,
            'reference_tree_size': 0,
            'reference_cache_hit_rate': 0.0,
            'move_agreement': 0
        }

    return {
        'reference_move': reference_stats.get('move'),
        'reference_rollouts': reference_stats.get('num_rollouts', 0),
        'reference_visits': reference_stats.get('num_visits', 0),
        'reference_time_ms': reference_stats.get('actual_time_ms', 0),
        'reference_nodes': reference_stats.get('nodes_visited', 0),
        'reference_max_depth': reference_stats.get('max_depth', 0),
        'reference_avg_depth': reference_stats.get('avg_depth', 0.0),
        'reference_tree_size': reference_stats.get('tree_size', 0),
        'reference_cache_hit_rate': reference_stats.get('cache_hit_rate', 0.0),
        'move_agreement': 1 if reference_stats.get('move_agreement') else 0
    }


def extract_reference_policy(reference_stats: Dict) -> Dict[str, float]:
    """Extract reference agent policy."""
    if not reference_stats:
        return {}

    visit_counts = reference_stats.get('visit_counts', {})

    policy = {}
    for col, count in visit_counts.items():
        policy[f'ref_policy_col_{col}'] = float(count)

    return policy


def flatten_event_for_parquet(event: Dict) -> Dict[str, Any]:
    """
    Flatten event structure for parquet storage.
    Preserves boardBefore as nested list for later processing.
    """
    flattened = {
        'eventId': event.get('eventId'),
        'gameId': event.get('gameId'),
        'moveIndex': event.get('moveIndex'),
        'player': event.get('player'),
        'playerType': event.get('playerType'),
        'actionTaken': event.get('actionTaken'),

        # Keep boards as nested lists (will be preserved in parquet)
        'boardBefore': event.get('boardBefore'),
        'boardAfter': event.get('boardAfter'),
        'legalActions': event.get('legalActions'),

        'thinkingTimeMs': event.get('thinkingTimeMs'),
        'timestamp': event.get('timestamp'),
    }

    # Extract MCTS features
    mcts_stats = event.get('mctsStats', {})
    flattened.update(extract_mcts_features(mcts_stats))
    flattened.update(extract_mcts_policy(mcts_stats))
    flattened.update(extract_mcts_q_values(mcts_stats))

    # Extract reference agent features
    reference_stats = event.get('referenceAgentStats')
    flattened.update(extract_reference_features(reference_stats))
    flattened.update(extract_reference_policy(reference_stats))

    return flattened


def write_parquet(events: List[Dict], output_dir: str) -> str:
    """
    Write events to parquet file.
    Boards are stored as nested lists for later ML processing.
    """
    if not events:
        logger.warning("No events to write")
        return None

    Path(output_dir).mkdir(parents=True, exist_ok=True)

    # Flatten events
    flattened_events = [flatten_event_for_parquet(event) for event in events]

    # Create DataFrame
    df = pd.DataFrame(flattened_events)

    # Generate filename
    timestamp = int(datetime.utcnow().timestamp())
    filename = f"moves_{timestamp}.parquet"
    filepath = os.path.join(output_dir, filename)

    # Write to parquet
    df.to_parquet(filepath, engine='pyarrow', compression='snappy')

    logger.info(f"Wrote {len(events)} events to {filepath}")
    return filepath


def write_dataset(
    moves_dir: str,
    outcomes: List[Dict],
    output_path: str,
    version: str,
    use_dvc: bool = False
) -> Dict[str, Any]:
    """
    Create ML-ready dataset from move parquet files.
    Boards are converted to flattened integer features here.
    """
    logger.info(f"Creating dataset {version} from {moves_dir}")

    # Find all parquet files
    parquet_files = sorted([
        os.path.join(moves_dir, f)
        for f in os.listdir(moves_dir)
        if f.endswith('.parquet')
    ])

    if not parquet_files:
        return {
            'status': 'error',
            'message': 'No parquet files found'
        }

    logger.info(f"Found {len(parquet_files)} move files")

    # Load all moves
    dfs = []
    for file in parquet_files:
        df = pd.read_parquet(file)
        dfs.append(df)

    moves_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Loaded {len(moves_df)} total moves")

    # ✅ CRITICAL: Flatten boards AFTER loading from parquet
    # This is where the conversion happens!
    board_before_features = moves_df['boardBefore'].apply(
        lambda board: flatten_board_for_ml(board, 'board_before')
    )
    board_after_features = moves_df['boardAfter'].apply(
        lambda board: flatten_board_for_ml(board, 'board_after')
    )

    # Convert to DataFrames
    board_before_df = pd.DataFrame(board_before_features.tolist())
    board_after_df = pd.DataFrame(board_after_features.tolist())

    # Drop original nested columns
    moves_df = moves_df.drop(columns=['boardBefore', 'boardAfter'])

    # Concatenate features
    final_df = pd.concat([moves_df, board_before_df, board_after_df], axis=1)

    # Add metadata
    final_df['dataset_version'] = version
    final_df['created_at'] = datetime.utcnow().isoformat()

    # Create game outcomes lookup
    outcome_dict = {o['gameId']: o for o in outcomes}

    # Add outcome columns
    final_df['game_outcome'] = final_df['gameId'].map(
        lambda gid: outcome_dict.get(gid, {}).get('outcome')
    )
    final_df['game_winner'] = final_df['gameId'].map(
        lambda gid: outcome_dict.get(gid, {}).get('winner')
    )

    # Save
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    final_df.to_parquet(output_path, engine='pyarrow', compression='snappy')

    file_size_mb = os.path.getsize(output_path) / (1024 * 1024)

    # Calculate statistics
    stats = {
        'version': version,
        'total_moves': len(final_df),
        'total_games': final_df['gameId'].nunique(),
        'output_path': output_path,
        'file_size_mb': file_size_mb,
        'created_at': datetime.utcnow().isoformat(),
        'num_features': len(final_df.columns),
    }

    # Feature counts
    board_features = len([c for c in final_df.columns if 'board_' in c])
    mcts_features = len([c for c in final_df.columns if any(
        x in c for x in ['rollouts', 'visits', 'policy', 'q_value', 'depth', 'nodes']
    )])
    reference_features = len([c for c in final_df.columns if 'reference' in c or 'ref_' in c])

    stats['feature_counts'] = {
        'board_features': board_features,
        'mcts_features': mcts_features,
        'reference_features': reference_features,
        'total_features': len(final_df.columns)
    }

    # Outcome statistics
    if outcomes:
        wins = sum(1 for o in outcomes if o.get('outcome') == 'WIN')
        losses = sum(1 for o in outcomes if o.get('outcome') == 'LOSS')
        draws = sum(1 for o in outcomes if o.get('outcome') == 'DRAW')

        stats['wins'] = wins
        stats['losses'] = losses
        stats['draws'] = draws
        stats['win_rate'] = wins / len(outcomes) if outcomes else 0

    # Move statistics
    stats['avg_moves_per_game'] = final_df['gameId'].value_counts().mean()
    stats['min_moves_per_game'] = final_df['gameId'].value_counts().min()
    stats['max_moves_per_game'] = final_df['gameId'].value_counts().max()

    # Expert agreement (if reference moves available)
    if 'move_agreement' in final_df.columns:
        stats['expert_agreement_rate'] = final_df['move_agreement'].mean()
    else:
        stats['expert_agreement_rate'] = 0.0

    logger.info(f"Dataset statistics: {stats}")

    return stats