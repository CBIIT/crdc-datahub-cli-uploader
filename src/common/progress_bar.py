from tqdm import tqdm  # For progress bar
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, TransferSpeedColumn
"""
class to display progress
"""
class ProgressPercentage:
    def __init__(self, file_size):
        self._size = file_size
        self._seen_so_far = 0
        self._progress, self._task = create_progress_bar()  # Ensure task is stored

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        self._progress.update(self._task, advance=bytes_amount)  # Properly update progress
        self._progress.refresh()  # Force update to make sure it appears

    def __del__(self):
        self._progress.stop()  # Properly stop progress bar

class ProgressCallback:
    def __init__(self, file_size, progress, task_id):
        self.file_size = file_size
        self.progress = progress
        self.task_id = task_id
        self.bytes_transferred = 0

    def __call__(self, bytes_amount):
        """Update the progress bar based on bytes uploaded."""
        self.bytes_transferred += bytes_amount
        self.progress.update(self.task_id, completed=self.bytes_transferred)

def create_progress_bar():
    return Progress(
        TextColumn("Progress:"),
        BarColumn(bar_width=80, style="green"),
        TextColumn("[bold green]{task.percentage:>3.0f}%"),
        TextColumn("| {task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TextColumn("Elapsed:"),
        TimeRemainingColumn(),
        TextColumn("Remaining: [yellow]{task.time_remaining}"),
        TransferSpeedColumn()
    )