import threading
import os
from contextlib import asynccontextmanager
from typing import Optional
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from app.buffer import MoveBuffer, GameOutcomeBuffer
from app.parquet_writer import write_parquet, write_dataset
from app.consumer import RabbitConsumer
from app.dvc_manager import DVCManager
from app.config import *
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("logger")

# Initialize buffers
move_buffer = MoveBuffer(FLUSH_SIZE)
outcome_buffer = GameOutcomeBuffer()

# Initialize DVC Manager
dvc_manager = None
if USE_DVC:
    try:
        dvc_manager = DVCManager(
            workspace_dir="/workspace",
            dataset_dir=DATASET_DIR,
            remote_name=DVC_REMOTE,
            minio_endpoint=MINIO_ENDPOINT,
            minio_access_key=MINIO_ACCESS_KEY,
            minio_secret_key=MINIO_SECRET_KEY,
            minio_bucket=MINIO_BUCKET
        )
        logger.info(" DVC Manager initialized")
    except Exception as e:
        logger.error(f"Failed to initialize DVC Manager: {e}")
        dvc_manager = None


def start_consumer():
    """Start the RabbitMQ consumer in a separate thread."""
    logger.info("start_consumer() called")
    consumer = RabbitConsumer(move_buffer, outcome_buffer)
    consumer.start()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Application lifespan manager."""
    logger.info("LIFESPAN STARTUP BEGIN")

    # Start consumer thread
    thread = threading.Thread(
        target=start_consumer,
        daemon=True
    )
    thread.start()

    logger.info("LIFESPAN STARTUP END")

    yield

    logger.info("LIFESPAN SHUTDOWN - Flushing buffers")

    # Flush remaining moves
    events = move_buffer.flush()
    if events:
        write_parquet(events, PARQUET_DIR)
        logger.info(f"Flushed {len(events)} remaining moves")


app = FastAPI(
    title="AI Gameplay Logging Service",
    description="Service for logging AI gameplay moves and outcomes with DVC support",
    version="1.0.0",
    lifespan=lifespan
)


# Request/Response Models
class FlushResponse(BaseModel):
    flushed_events: int
    file: Optional[str]


class DatasetExportRequest(BaseModel):
    version: str
    min_games: Optional[int] = None
    auto_push_dvc: Optional[bool] = True
    cleanup_after_export: Optional[bool] = True  # NEW: Auto cleanup parquet files


class DatasetExportResponse(BaseModel):
    status: str
    version: str
    total_moves: int
    total_games: int
    output_path: str
    file_size_mb: float
    wins: Optional[int] = None
    losses: Optional[int] = None
    draws: Optional[int] = None
    win_rate: Optional[float] = None
    avg_moves_per_game: Optional[float] = None
    dvc_tracked: Optional[bool] = None
    dvc_pushed: Optional[bool] = None
    cleaned_up: Optional[bool] = None  # NEW: Whether cleanup was performed


class StatsResponse(BaseModel):
    move_buffer_size: int
    total_games: int
    wins: int
    losses: int
    draws: int
    win_rate: float
    parquet_files: int
    total_parquet_size_mb: float


# Endpoints
@app.get("/")
def root():
    """Health check endpoint."""
    return {
        "service": "AI Gameplay Logging Service",
        "status": "running",
        "version": "1.0.0",
        "dvc_enabled": USE_DVC,
        "dvc_configured": dvc_manager is not None
    }


@app.get("/health")
def health_check():
    """Detailed health check."""
    return {
        "status": "healthy",
        "move_buffer_size": move_buffer.size(),
        "game_outcomes": outcome_buffer.stats(),
        "dvc_enabled": USE_DVC,
        "dvc_ready": dvc_manager is not None
    }


@app.post("/flush", response_model=FlushResponse)
def flush_now():
    """Manually flush move buffer to parquet file."""
    events = move_buffer.flush()
    path = write_parquet(events, PARQUET_DIR)

    return FlushResponse(
        flushed_events=len(events),
        file=path
    )


@app.get("/stats", response_model=StatsResponse)
def get_stats():
    """Get current statistics about logged data."""
    # Get outcome stats
    outcome_stats = outcome_buffer.stats()

    # Get parquet file stats
    parquet_files = []
    total_size = 0

    if os.path.exists(PARQUET_DIR):
        parquet_files = [
            f for f in os.listdir(PARQUET_DIR)
            if f.endswith('.parquet')
        ]
        total_size = sum(
            os.path.getsize(os.path.join(PARQUET_DIR, f))
            for f in parquet_files
        )

    return StatsResponse(
        move_buffer_size=move_buffer.size(),
        total_games=outcome_stats['total_games'],
        wins=outcome_stats['wins'],
        losses=outcome_stats['losses'],
        draws=outcome_stats['draws'],
        win_rate=outcome_stats['win_rate'],
        parquet_files=len(parquet_files),
        total_parquet_size_mb=total_size / (1024 * 1024)
    )


@app.post("/dataset/export", response_model=DatasetExportResponse)
def export_dataset(request: DatasetExportRequest):
    """Export collected data as a training-ready dataset with automatic DVC tracking."""
    # Check if we have enough games
    outcome_stats = outcome_buffer.stats()
    min_games = request.min_games or MIN_GAMES_FOR_EXPORT

    if outcome_stats['total_games'] < min_games:
        raise HTTPException(
            status_code=400,
            detail=f"Insufficient games collected. Required: {min_games}, "
                   f"Available: {outcome_stats['total_games']}"
        )

    # First, flush any remaining moves
    events = move_buffer.flush()
    if events:
        write_parquet(events, PARQUET_DIR)
        logger.info(f"Flushed {len(events)} moves before export")

    # Prepare output path
    output_filename = f"dataset_{request.version}.parquet"
    output_path = os.path.join(DATASET_DIR, output_filename)

    # Get all game outcomes
    outcomes = outcome_buffer.get_all_outcomes()

    # Create dataset (DVC tracking disabled here, we'll do it manually after)
    try:
        stats = write_dataset(
            moves_dir=PARQUET_DIR,
            outcomes=outcomes,
            output_path=output_path,
            version=request.version,
            use_dvc=False  # We'll handle DVC manually
        )

        if stats.get('status') == 'error':
            raise HTTPException(status_code=500, detail=stats.get('message'))

        logger.info(f"Dataset {request.version} exported successfully")

        # DVC tracking and push
        dvc_tracked = False
        dvc_pushed = False

        if USE_DVC and dvc_manager and request.auto_push_dvc:
            logger.info("Tracking dataset with DVC...")

            # Track with DVC
            dvc_tracked = dvc_manager.track_file(output_path)

            if dvc_tracked:
                # Push to MinIO
                logger.info("Pushing dataset to MinIO...")
                dvc_pushed = dvc_manager.push()

                if dvc_pushed:
                    logger.info(" Dataset pushed to MinIO via DVC")
                else:
                    logger.warning(" DVC push failed - dataset tracked but not pushed")
            else:
                logger.warning(" DVC tracking failed")

        stats['dvc_tracked'] = dvc_tracked
        stats['dvc_pushed'] = dvc_pushed

        # NEW: Cleanup parquet files after successful export
        cleaned_up = False
        if request.cleanup_after_export:
            logger.info("Cleaning up parquet files after successful export...")

            parquet_files_deleted = 0
            if os.path.exists(PARQUET_DIR):
                for f in os.listdir(PARQUET_DIR):
                    if f.endswith('.parquet'):
                        try:
                            os.remove(os.path.join(PARQUET_DIR, f))
                            parquet_files_deleted += 1
                        except Exception as e:
                            logger.error(f"Failed to delete {f}: {e}")

                logger.info(f"Deleted {parquet_files_deleted} parquet files from {PARQUET_DIR}")

            # Clear outcome buffer
            outcome_buffer.clear_outcomes()
            logger.info("Cleared game outcome buffer")

            cleaned_up = True
            logger.info(" Cleanup complete - ready for next dataset generation")

        stats['cleaned_up'] = cleaned_up

        return DatasetExportResponse(
            status="success",
            **stats
        )

    except Exception as e:
        logger.error(f"Failed to export dataset: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/parquet/cleanup")
def cleanup_parquet_files():
    """Manually cleanup parquet files and outcome buffer."""
    parquet_files_deleted = 0

    if os.path.exists(PARQUET_DIR):
        for f in os.listdir(PARQUET_DIR):
            if f.endswith('.parquet'):
                try:
                    os.remove(os.path.join(PARQUET_DIR, f))
                    parquet_files_deleted += 1
                except Exception as e:
                    logger.error(f"Failed to delete {f}: {e}")

    # Clear outcome buffer
    cleared_outcomes = outcome_buffer.clear_outcomes()

    logger.info(
        f"Manual cleanup: deleted {parquet_files_deleted} parquet files, cleared {len(cleared_outcomes)} outcomes")

    return {
        "status": "success",
        "parquet_files_deleted": parquet_files_deleted,
        "outcomes_cleared": len(cleared_outcomes),
        "message": f"Deleted {parquet_files_deleted} parquet files and cleared {len(cleared_outcomes)} game outcomes"
    }


@app.get("/outcomes")
def get_outcomes():
    """Get all game outcomes."""
    return {
        "outcomes": outcome_buffer.get_all_outcomes(),
        "stats": outcome_buffer.stats()
    }


@app.delete("/outcomes/clear")
def clear_outcomes():
    """Clear all stored game outcomes."""
    cleared = outcome_buffer.clear_outcomes()
    return {
        "cleared": len(cleared),
        "message": f"Cleared {len(cleared)} game outcomes"
    }


@app.get("/dvc/status")
def dvc_status():
    """Get DVC status."""
    if not dvc_manager:
        return {
            "enabled": False,
            "message": "DVC not initialized"
        }

    status = dvc_manager.status()
    return {
        "enabled": True,
        **status
    }


@app.post("/dvc/push")
def dvc_push():
    """Manually push to DVC remote."""
    if not dvc_manager:
        raise HTTPException(status_code=503, detail="DVC not initialized")

    success = dvc_manager.push()

    if success:
        return {"status": "success", "message": "Pushed to DVC remote"}
    else:
        raise HTTPException(status_code=500, detail="DVC push failed")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)