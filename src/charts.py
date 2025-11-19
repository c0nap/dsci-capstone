import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


class Plot:
    """Static plotting helpers for visualization."""

    @staticmethod
    def time_elapsed_by_names(df: pd.DataFrame, filename: str) -> None:
        """Plot average elapsed time per function name, averaging across runs.
        @param df  DataFrame with columns ['name', 'elapsed', 'call_chain', 'run_id']
        @param filename  Where to save the generated chart
        """
        # 1. Average per-run per-function (handles multiple calls in a run)
        per_run_avg = df.groupby(['run_id', 'name'])['elapsed'].mean().reset_index()
        
        # 2. Average across all runs
        overall_avg = per_run_avg.groupby('name')['elapsed'].mean().reset_index()
        
        # 3. Plot
        sns.barplot(data=overall_avg, x='name', y='elapsed')
        plt.xticks(rotation=45, ha='right')
        plt.ylabel("Average elapsed time")
        plt.xlabel("Function name")
        plt.title("Average Function Runtime Across Runs")
        plt.tight_layout()
        
        # 4. Save the figure
        plt.savefig(filename)
        plt.close()
