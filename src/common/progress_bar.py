from rich.progress import (Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, TransferSpeedColumn,
                           DownloadColumn)

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
        BarColumn(bar_width=80, style="grey50", complete_style="green"),
        TextColumn("[bold green]{task.percentage:>3.0f}%"),
        DownloadColumn(),
        TextColumn("Elapsed:"),
        TimeElapsedColumn(),
        TextColumn("Remaining:"),
        TimeRemainingColumn(),
        TransferSpeedColumn()
    )