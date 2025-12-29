import threading
from typing import List, Dict, Optional
from datetime import datetime


class MoveBuffer:
    """Thread-safe buffer for move events with auto-flush capability."""

    def __init__(self, max_size: int):
        self.buffer: List[Dict] = []
        self.lock = threading.Lock()
        self.max_size = max_size

    def add(self, event: Dict) -> bool:
        """
        Add an event to the buffer.

        Args:
            event: The move event to add

        Returns:
            bool: True if buffer should be flushed, False otherwise
        """
        with self.lock:
            self.buffer.append(event)
            return len(self.buffer) >= self.max_size

    def flush(self) -> List[Dict]:
        """
        Flush all events from the buffer.

        Returns:
            List of events that were in the buffer
        """
        with self.lock:
            data = self.buffer
            self.buffer = []
            return data

    def size(self) -> int:
        """Get current buffer size."""
        with self.lock:
            return len(self.buffer)


class GameOutcomeBuffer:
    """Thread-safe buffer for game outcome events."""

    def __init__(self):
        self.games: Dict[str, Dict] = {}
        self.lock = threading.Lock()
        self.completed_games: List[Dict] = []

    def add_game_outcome(self, outcome: Dict) -> None:
        """
        Add a game outcome event.

        Args:
            outcome: Game outcome data including gameId, winner, outcome, etc.
        """
        with self.lock:
            game_id = outcome.get('gameId')
            if game_id:
                self.games[game_id] = {
                    'gameId': game_id,
                    'outcome': outcome.get('outcome'),
                    'winner': outcome.get('winner'),
                    'totalMoves': outcome.get('totalMoves'),
                    'gameDurationMs': outcome.get('gameDurationMs'),
                    'timestamp': outcome.get('timestamp', datetime.utcnow().isoformat()),
                    'phase': outcome.get('phase'),
                    'player1': outcome.get('player1', 'player1'),
                    'player2': outcome.get('player2', 'player2'),
                    'player1Type': outcome.get('player1Type', 'ai'),
                    'player2Type': outcome.get('player2Type', 'ai'),
                }
                self.completed_games.append(self.games[game_id])

    def get_game_outcome(self, game_id: str) -> Optional[Dict]:
        """Get outcome for a specific game."""
        with self.lock:
            return self.games.get(game_id)

    def get_all_outcomes(self) -> List[Dict]:
        """Get all completed game outcomes."""
        with self.lock:
            return self.completed_games.copy()

    def clear_outcomes(self) -> List[Dict]:
        """Clear and return all game outcomes."""
        with self.lock:
            outcomes = self.completed_games
            self.completed_games = []
            return outcomes

    def stats(self) -> Dict:
        """Get statistics about completed games."""
        with self.lock:
            if not self.completed_games:
                return {
                    'total_games': 0,
                    'wins': 0,
                    'losses': 0,
                    'draws': 0,
                    'win_rate': 0
                }

            wins = sum(1 for g in self.completed_games if g.get('outcome') == 'WIN')
            losses = sum(1 for g in self.completed_games if g.get('outcome') == 'LOSS')
            draws = sum(1 for g in self.completed_games if g.get('outcome') == 'DRAW')

            return {
                'total_games': len(self.completed_games),
                'wins': wins,
                'losses': losses,
                'draws': draws,
                'win_rate': wins / len(self.completed_games) if self.completed_games else 0
            }
