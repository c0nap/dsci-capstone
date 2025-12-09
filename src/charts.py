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
        ax.barh(y_pos, -merged['elapsed_left'], align='center', label='Improved', color='tab:blue')
        ax.barh(y_pos, merged['elapsed_right'], align='center', label='Original', color='tab:orange')

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



    METRIC_NAMES: Dict[str, str] = {
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
        """Generate a bar chart showing metrics for a single summary."""
        # Convert keys to display names
        names = [METRIC_NAMES[k] for k in metrics.keys()]
        values = [metrics[k] for k in metrics.keys()]

        plt.figure(figsize=(len(metrics) * 0.8, 8))
        plt.barh(names, values)
        plt.xlabel("Score")
        plt.title("Evaluation of Single Summary")
        plt.tight_layout()
        plt.show()


    # TODO: refactor
    @staticmethod
    def save_metrics_csv(metrics: Dict[str, float], filename: str = "./logs/metrics/chunk_summary.csv") -> None:
        """Save a metrics dict to CSV using pandas."""
        # Convert to a simple 2-column DataFrame
        df = pd.DataFrame([
            {"metric": key, "value": value}
            for key, value in metrics.items()
        ])

        # Ensure directory exists
        os.makedirs(os.path.dirname(filename), exist_ok=True)

        # Save CSV
        df.to_csv(filename, index=False)

        # Optional: log the output path
        Log.chart("Saved summary metrics CSV", filename)


    @staticmethod
    def summary_comparison(filename: str, paths: list[str], fixed_colors: List[str], labels: List[str]) -> None:
        """Compare metrics across an arbitrary number of metric CSV files."""
        import matplotlib.pyplot as plt

        merged = None

        # Load each CSV and align metrics
        for i, path in enumerate(paths):
            # Determine label
            if i < len(labels):
                label = labels[i]
            else:
                label = os.path.splitext(os.path.basename(path))[0]
                labels.append(label)

            df = pd.read_csv(path)   # expects columns: metric, value
            df = df.rename(columns={"value": label})

            if merged is None:
                merged = df
            else:
                merged = pd.merge(merged, df, on="metric", how="outer")

        # Pretty names for metrics
        merged["metric"] = merged["metric"].apply(
            lambda k: Plot.METRIC_NAMES.get(k, k)
        )
        merged = merged.sort_values("metric")

        # Plot setup
        plt.figure(figsize=(12, len(merged) * 0.6))

        y_positions = range(len(merged))
        bar_height = 0.8 / len(labels)

        # Draw bars for each dataset
        for i, label in enumerate(labels):
            if i < len(fixed_colors):
                color = fixed_colors[i]
            else:
                color = None  # auto color

            offset = (i - (len(labels) - 1) / 2) * bar_height

            plt.barh(
                [y + offset for y in y_positions],
                merged[label],
                height=bar_height,
                label=label,
                color=color,
            )

        plt.yticks(range(len(merged)), merged["metric"])
        plt.xlabel("Score")
        plt.title("Metric Comparison Across Summaries")
        plt.legend()
        plt.tight_layout()

        # Save
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plt.savefig(filename)
        plt.close()

        Log.chart("Metric Comparison", filename)





if __name__ == "__main__":
    # plot_time_comparison()
    plot_metrics_comparison()


def plot_time_comparison():
    # make docker-python-dev CMD="src.charts './logs/elapsed_time_best.csv' './logs/elapsed_time_worst.csv' --output='./logs/charts/runtime_comparison.png'"
    import argparse

    parser = argparse.ArgumentParser(description='Compare function runtimes from two CSV files')
    parser.add_argument('csv1', help='Path to first CSV file')
    parser.add_argument('csv2', help='Path to second CSV file')
    parser.add_argument('--output', default='./logs/charts/runtime_comparison.png', help='Output filename for chart')

    args = parser.parse_args()

    Plot.time_elapsed_comparison(filename=args.output, csv1=args.csv1, csv2=args.csv2, only_pipeline=False, log_scale=False, cap_outliers=0.06)

def plot_metrics_comparison():
    # make docker-python-dev CMD="src.charts './logs/metrics/chunk_summary_best.csv' './logs/metrics/chunk_summary_worst.csv' './logs/metrics/chunk_summary_llm.csv' --output='./logs/charts/metrics_comparison.png'"
    import argparse

    parser = argparse.ArgumentParser(description='Compare function runtimes from two CSV files')
    parser.add_argument('csv1', help='Path to first CSV file')
    parser.add_argument('csv2', help='Path to second CSV file')
    parser.add_argument('csv3', help='Path to third CSV file')
    parser.add_argument('--output', default='./logs/charts/runtime_comparison.png', help='Output filename for chart')

    args = parser.parse_args()

    Plot.summary_comparison(filename=args.output, paths=[args.csv1, args.csv2, args.csv3], fixed_colors=["tab:blue", "tab:orange", "tab:green"], labels=["Best", "Fast", "LLM-Only"])



