import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns
from src.util import Log
from typing import Optional


class Plot:
    """Static plotting helpers for visualization."""

    @staticmethod
    def time_elapsed_horizontal(filename: str = "./logs/charts/avg_runtime.png") -> None:
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
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plt.savefig(filename)
        plt.close()
        Log.chart(title, filename)

    @staticmethod
    def time_elapsed_by_names(filename: str = "./logs/charts/avg_runtime.png") -> None:
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
        sns.barplot(data=overall_avg, y='function', x='elapsed', orient='h')
        plt.xlabel("Average elapsed time")
        plt.ylabel("Function name")
        plt.title(title)
        plt.tight_layout()

        # 4. Save the figure
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plt.savefig(filename)
        plt.close()
        Log.chart(title, filename)

    @staticmethod
    def time_elapsed_comparison(
        filename: str = "./logs/charts/runtime_comparison.png",
        csv1: str = None,
        csv2: str = None,
        only_pipeline: Optional[bool] = None,
        log_scale: bool = False,
        cap_outliers: float = 0,
    ) -> None:
        """Plot average elapsed time per function name from two CSV files as mirrored horizontal bars.
        @param filename  Where to save the generated chart
        @param csv1  Path to first CSV file (left bars)
        @param csv2  Path to second CSV file (right bars)
        @param only_pipeline  Include pipeline_A (True), task_40 (False), or both (None)
        @param log_scale  Whether to use a logarithmic scale for the plot
        @param cap_outliers  Percentile to truncate large outliers. Disabled at 0 by default.
        """
        # Read data from CSV files or fall back to Log.get_merged_timing()
        if csv1 and csv2:
            df1 = pd.read_csv(csv1)
            df2 = pd.read_csv(csv2)
        else:
            raise

        # Process both datasets
        def process_df(df: pd.DataFrame, only_pipeline: bool) -> pd.DataFrame:
            # Choose or exclude functions containing "pipeline"
            if only_pipeline:
                df = df[df['function'].str.contains('pipeline', case=False, na=False)]
            else:
                df = df[~df['function'].str.contains('pipeline', case=False, na=False)]
            per_run_avg = df.groupby(['run_id', 'function'])['elapsed'].mean().reset_index()
            return per_run_avg.groupby('function')['elapsed'].mean().reset_index()

        if only_pipeline is not None:
            avg1 = process_df(df1, only_pipeline)
            avg2 = process_df(df2, only_pipeline)

        # Merge on function names to align bars
        merged = pd.merge(avg1, avg2, on='function', suffixes=('_left', '_right'))

        # Create figure
        fig, ax = plt.subplots(figsize=(10, len(merged) * 0.5))

        # Plot bars going inward from center
        y_pos = range(len(merged))
        ax.barh(y_pos, -merged['elapsed_left'], align='center', label='Improved')
        ax.barh(y_pos, merged['elapsed_right'], align='center', label='Original')

        # Configure axes with log scale to handle outliers
        if log_scale:
            ax.set_xscale('symlog', linthresh=1.0)
        if cap_outliers > 0:
            max_val = merged[['elapsed_left', 'elapsed_right']].quantile(1 - cap_outliers).max()
            ax.set_xlim(-max_val, max_val)  # cuts off top 5%
        ax.set_yticks(y_pos)
        ax.set_yticklabels(merged['function'])
        ax.axvline(0, color='black', linewidth=0.8)
        if log_scale:
            ax.set_xlabel("Average elapsed seconds (log scale)")
        else:
            ax.set_xlabel("Average elapsed time (seconds)")
        ax.set_title("Average Function Runtime Comparison")
        ax.legend()

        plt.tight_layout()

        # Save the figure
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plt.savefig(filename)
        plt.close()
        Log.chart("Average Function Runtime Comparison", filename)



    CORE_METRICS: Dict[str, str] = {
        "rougeL_recall" : "ROUGE-L (Recall)",
        "bertscore" : "BERTScore (F1)",
        "novel_ngrams" : "Novel N-Grams",
        "jsd_stats" : "JSD",
        "entity_coverage" : "entity_coverage",
        "entity_hallucination" : "entity_hallucination",
        "ncd_overlap" : "NCD",
        "salience_recall" : "Saliance (Recall)",
        "nli_faithfulness" : "Faithfulness",
        "readability_delta" : "Readability Delta",
        "sentence_coherence" : "Sentence Coherence",
        "entity_grid_coherence" : "Entity Grid Coherence",
        "lexical_diversity" : "Lexical Diversity (TTR)",
        "stopword_ratio" : "Stopword Ratio",
        "bookscore" : "BooookScore",
        "questeval" : "QuestEval",
    }

    @staticmethod
    def summary_results(metrics: Dict[str, float]) -> None:


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description='Compare function runtimes from two CSV files')
    parser.add_argument('csv1', help='Path to first CSV file')
    parser.add_argument('csv2', help='Path to second CSV file')
    parser.add_argument('--output', default='./logs/charts/runtime_comparison.png', help='Output filename for chart')

    args = parser.parse_args()

    Plot.time_elapsed_comparison(filename=args.output, csv1=args.csv1, csv2=args.csv2, only_pipeline=False, log_scale=False, cap_outliers=0.06)
