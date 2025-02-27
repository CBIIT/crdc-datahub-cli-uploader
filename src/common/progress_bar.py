from tqdm import tqdm  # For progress bar
from rich.progress import Progress, BarColumn, TextColumn, TimeRemainingColumn, TimeElapsedColumn, TransferSpeedColumn
"""
class to display progress
"""
class ProgressPercentage:
    def __init__(self, file_size):
        self._size = file_size
        self._seen_so_far = 0
        self._progress, self._task = create_progress_bar(file_size)  # Ensure task is stored

    def __call__(self, bytes_amount):
        self._seen_so_far += bytes_amount
        self._progress.update(self._task, advance=bytes_amount)  # Properly update progress
        self._progress.refresh()  # Force update to make sure it appears

    def __del__(self):
        self._progress.stop()  # Properly stop progress bar

def create_progress_bar(file_size):
    # progress_bar = tqdm(total= file_size, unit='B', unit_scale=True, desc="Progress", smoothing=0.0,
    #                           bar_format="{l_bar}\033[1;32m{bar}\033[0m| {n_fmt}/{total_fmt} [elapsed: {elapsed} | remaining: {remaining}, {rate_fmt}]")
    # return progress_bar
    progress = Progress(
        TextColumn("Progress:"),
        BarColumn(bar_width=None, style="green"),
        TextColumn("[bold green]{task.percentage:>3.0f}%"),
        TextColumn("| {task.completed}/{task.total}"),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        TransferSpeedColumn()
    )

    task = progress.add_task("Downloading", total=file_size)
    return progress, task