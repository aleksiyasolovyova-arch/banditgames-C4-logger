"""
DVC Manager for Logger Service

Handles DVC initialization, tracking, and pushing inside the Docker container.
No manual DVC commands needed - everything is automatic.
"""
import os
import subprocess
import logging
from pathlib import Path
from typing import Optional

logger = logging.getLogger("logger")


class DVCManager:
    """Manages DVC operations inside the logger container."""

    def __init__(
            self,
            workspace_dir: str = "/workspace",
            dataset_dir: str = "/data/datasets",
            remote_name: str = "minio",
            minio_endpoint: Optional[str] = None,
            minio_access_key: Optional[str] = None,
            minio_secret_key: Optional[str] = None,
            minio_bucket: str = "datasets"
    ):
        self.workspace_dir = Path(workspace_dir)
        self.dataset_dir = Path(dataset_dir)
        self.remote_name = remote_name
        self.minio_endpoint = minio_endpoint or os.getenv("MINIO_ENDPOINT", "http://minio:9000")
        self.minio_access_key = minio_access_key or os.getenv("MINIO_ACCESS_KEY")
        self.minio_secret_key = minio_secret_key or os.getenv("MINIO_SECRET_KEY")
        self.minio_bucket = minio_bucket

        # Initialize DVC on startup
        self._initialize()

    def _run_command(self, cmd: list, cwd: Optional[Path] = None) -> tuple[bool, str]:
        """Run a shell command and return success status and output."""
        try:
            result = subprocess.run(
                cmd,
                cwd=cwd or self.workspace_dir,
                capture_output=True,
                text=True,
                check=True
            )
            return True, result.stdout
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed: {' '.join(cmd)}\nError: {e.stderr}")
            return False, e.stderr
        except FileNotFoundError:
            logger.error(f"Command not found: {cmd[0]}")
            return False, f"Command not found: {cmd[0]}"

    def _initialize(self):
        """Initialize DVC if not already initialized."""
        dvc_dir = self.workspace_dir / ".dvc"

        if dvc_dir.exists():
            logger.info("DVC already initialized")
        else:
            logger.info("Initializing DVC...")
            success, output = self._run_command(["dvc", "init"])
            if success:
                logger.info(" DVC initialized")
            else:
                logger.error(f" DVC initialization failed: {output}")
                return

        # Configure remote
        self._configure_remote()

    def _configure_remote(self):
        """Configure MinIO as DVC remote."""
        if not self.minio_access_key or not self.minio_secret_key:
            logger.warning("MinIO credentials not set - DVC remote not configured")
            return

        logger.info(f"Configuring DVC remote: {self.remote_name}")

        # Add remote (ignore if already exists)
        self._run_command([
            "dvc", "remote", "add", "-d", "--force",
            self.remote_name,
            f"s3://{self.minio_bucket}"
        ])

        # Set endpoint
        self._run_command([
            "dvc", "remote", "modify",
            self.remote_name,
            "endpointurl",
            self.minio_endpoint
        ])

        # Set credentials
        self._run_command([
            "dvc", "remote", "modify",
            self.remote_name,
            "access_key_id",
            self.minio_access_key
        ])

        self._run_command([
            "dvc", "remote", "modify",
            self.remote_name,
            "secret_access_key",
            self.minio_secret_key
        ])

        logger.info(" DVC remote configured")

    def track_file(self, file_path: str) -> bool:
        """
        Track a file with DVC.

        Args:
            file_path: Path to file (relative to workspace or absolute)

        Returns:
            bool: True if successful
        """
        # Convert to Path object
        path = Path(file_path)

        # If absolute path outside workspace, make it relative
        if path.is_absolute():
            try:
                path = path.relative_to(self.workspace_dir)
            except ValueError:
                logger.error(f"File {file_path} is outside workspace {self.workspace_dir}")
                return False

        logger.info(f"Tracking file with DVC: {path}")

        success, output = self._run_command(["dvc", "add", str(path)])

        if success:
            logger.info(f" File tracked: {path}")
            return True
        else:
            logger.error(f" Failed to track file: {output}")
            return False

    def push(self) -> bool:
        """
        Push tracked files to DVC remote (MinIO).

        Returns:
            bool: True if successful
        """
        logger.info("Pushing to DVC remote...")

        success, output = self._run_command(["dvc", "push"])

        if success:
            logger.info(" Pushed to DVC remote")
            return True
        else:
            logger.error(f" DVC push failed: {output}")
            return False

    def pull(self) -> bool:
        """
        Pull files from DVC remote.

        Returns:
            bool: True if successful
        """
        logger.info("Pulling from DVC remote...")

        success, output = self._run_command(["dvc", "pull"])

        if success:
            logger.info(" Pulled from DVC remote")
            return True
        else:
            logger.error(f" DVC pull failed: {output}")
            return False

    def track_and_push(self, file_path: str) -> bool:
        """
        Track a file and immediately push to remote.

        Args:
            file_path: Path to file

        Returns:
            bool: True if both operations successful
        """
        if not self.track_file(file_path):
            return False

        return self.push()

    def status(self) -> dict:
        """Get DVC status."""
        success, output = self._run_command(["dvc", "status"])

        return {
            "success": success,
            "output": output,
            "remote_configured": bool(self.minio_access_key and self.minio_secret_key)
        }