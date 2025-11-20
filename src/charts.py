import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from src.util import Log


class Plot:
    """Static plotting helpers for visualization."""

    @staticmethod
    def time_elapsed_by_names(filename: str = "./logs/avg_runtime.png") -> None:
        """Plot average elapsed time per function name, averaging across runs.
        @param filename  Where to save the generated chart
        """
        df = Log.get_merged_timing()  # DataFrame with columns ['function', 'elapsed', 'call_chain', 'run_id']
        # 1. Average per-run per-function (handles multiple calls in a run)
        per_run_avg = df.groupby(['run_id', 'function'])['elapsed'].mean().reset_index()

        # 2. Average across all runs
        overall_avg = per_run_avg.groupby('function')['elapsed'].mean().reset_index()

        # 3. Plot
        title = "Average Function Runtime Across Runs"
        sns.barplot(data=overall_avg, x='function', y='elapsed')
        plt.xticks(rotation=45, ha='right')
        plt.ylabel("Average elapsed time")
        plt.xlabel("Function name")
        plt.title(title)
        plt.tight_layout()

        # 4. Save the figure
        plt.savefig(filename)
        plt.close()
        Log.chart(title, filename)
