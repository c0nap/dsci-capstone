import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns
from src.util import Log
from typing import Optional, Dict, List


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
            df1 = process_df(df1, only_pipeline)
            df2 = process_df(df2, only_pipeline)

        # Merge on function names to align bars
        merged = pd.merge(df1, df2, on='function', suffixes=('_left', '_right'))
        merged = merged.sort_values(by=['function'], ascending=False).reset_index(drop=True)

        # Create figure
        height = max(1, len(merged) * 0.4)
        fig, ax = plt.subplots(figsize=(10, height))

        # Plot bars going inward from center
        y_pos = range(len(merged))
        ax.barh(y_pos, -merged['elapsed_left'], align='center', label='Best', color='tab:blue')
        ax.barh(y_pos, merged['elapsed_right'], align='center', label='Fast', color='tab:orange')

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
        title = "Function Runtime Comparison (Single Chunk)"
        ax.set_title(title)
        ax.legend()

        plt.tight_layout()

        # Save the figure
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plt.savefig(filename)
        plt.close()
        Log.chart(title, filename)



    METRIC_NAMES: Dict[str, str] = {
        "rougeL_recall" : "ROUGE-L (Recall)",
        "bertscore" : "Clamped BERTScore (F1)",
        "novel_ngrams" : "Inverted N-Gram Hallucination",
        "jsd_stats" : "Inverted JSD",
        "entity_coverage" : "Entity Coverage",
        "entity_hallucination" : "Inverted Entity Hallucination",
        "ncd_overlap" : "Inverted NCD Similarity",
        "salience_recall" : "Salience (Recall)",
        "nli_faithfulness" : "Entailed Faithfulness",
        "readability_delta" : "Clamped Readability Delta",
        "sentence_coherence" : "Sentence Coherence",
        "entity_grid_coherence" : "Entity Grid Coherence",
        "lexical_diversity" : "Lexical Diversity (TTR)",
        "stopword_ratio" : "Stopword Ratio",
        "bookscore" : "BooookScore Coherence",
        "questeval" : "QuestEval Factuality",
    }
    def normalize_metrics(metrics: Dict[str, float]) ->  Dict[str, float]:
        for key, value in metrics.items():
            if key == "readability_delta":
                # Clamp to [0, 1] range
                bound = 10
                normalized = max(-bound, min(bound, value))
                # map [-10, +10] -> [0, 1]
                normalized = (normalized + bound) / (2 * bound)
                metrics[key] = normalized
            if key == "bertscore":
                # Clamp to [0, 1] range
                normalized = max(0.0, min(1.0, value))
                metrics[key] = normalized
            if key in ["jsd_stats", "novel_ngrams", "ncd_overlap", "entity_hallucination"]:
                metrics[key] = 1 - value

    @staticmethod
    def summary_results(metrics: Dict[str, float]) -> None:
        """Generate a bar chart showing metrics for a single summary."""
        # Convert keys to display names
        names = [Plot.METRIC_NAMES[k] for k in metrics.keys()]
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



    METRIC_GROUPS = [
        ("BASIC COMPARISON", ["rougeL_recall", "bertscore", "novel_ngrams", "jsd_stats", "entity_coverage"]),
        ("HIGH-LEVEL COMPARISON", ["ncd_overlap", "salience_recall", "nli_faithfulness", "readability_delta"]),
        ("REFERENCE-FREE", ["sentence_coherence", "entity_grid_coherence", "lexical_diversity", "stopword_ratio", "bookscore", "questeval"]),
    ]


    @staticmethod
    def summary_comparison(filename: str, paths: list[str], fixed_colors: List[str], labels: List[str]) -> None:
        """Compare metrics across an arbitrary number of metric CSV files, with grouped metric labels."""
        import matplotlib.pyplot as plt
        import numpy as np
    
        merged = None
    
        # Load each CSV and align metrics
        for i, path in enumerate(paths):
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
    
        # Normalize metrics for each label column
        for col in merged.columns:
            if col != "metric":
                merged[col] = Plot.normalize_metrics(merged[col].to_dict())
                merged[col] = pd.Series(merged[col])

        # Pretty names for metrics
        merged["metric"] = merged["metric"].apply(lambda k: Plot.METRIC_NAMES.get(k, k))
    
        # Compute y positions and insert extra spacing between groups
        y_positions = []
        y_labels = []
        group_ticks = []
        spacing = 0.5  # extra space between groups
        y = 0
        for group_name, metrics in Plot.METRIC_GROUPS:
            group_indices = []
            for m in metrics:
                # Find metric index in merged
                idx = merged.index[merged["metric"] == Plot.METRIC_NAMES.get(m, m)].tolist()
                if idx:
                    y_positions.append(y)
                    y_labels.append(merged.loc[idx[0], "metric"])
                    group_indices.append(y)
                    y += 1
            if group_indices:
                group_center = np.mean(group_indices)
                group_ticks.append((group_center, group_name))
                y += spacing  # add extra space after group
    
        bar_height = 0.8 / len(labels)
    
        plt.figure(figsize=(12, y * 0.6))
    
        # Draw bars
        for i, label in enumerate(labels):
            color = fixed_colors[i] if i < len(fixed_colors) else None
            offset = (i - (len(labels) - 1) / 2) * bar_height
            values = []
            for lbl in y_labels:
                # Fetch value by metric name
                match = merged[merged["metric"] == lbl][label]
                values.append(match.values[0] if not match.empty else 0)
            plt.barh([y + offset for y in range(len(values))], values, height=bar_height, label=label, color=color)
    
        plt.yticks(range(len(y_labels)), y_labels)
        plt.xlabel("Score")
        title = "Quality Comparison (Chunk-Level Summary)"
        plt.title(title)
        plt.legend()
    
        # Draw vertical dotted lines between groups
        for idx, (center, name) in enumerate(group_ticks[:-1]):
            plt.axhline(y=center + 0.5, color="gray", linestyle="dotted")  # dotted line between groups
    
        # Add group labels at top interior
        for center, name in group_ticks:
            plt.text(-0.05 * max(merged[labels].max().max(), 1), center, name, fontsize=10, fontweight="bold", va="center", ha="right")
    
        plt.tight_layout()
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plt.savefig(filename)
        plt.close()
    
        Log.chart(title, filename)







def plot_time_comparison():
    # python -m src.charts './logs/elapsed_time_best.csv' './logs/elapsed_time_worst.csv' --output='./logs/charts/runtime_comparison.png'
    import argparse

    parser = argparse.ArgumentParser(description='Compare function runtimes from two CSV files')
    parser.add_argument('csv1', help='Path to first CSV file')
    parser.add_argument('csv2', help='Path to second CSV file')
    parser.add_argument('--output', default='./logs/charts/runtime_comparison.png', help='Output filename for chart')

    args = parser.parse_args()

    Plot.time_elapsed_comparison(filename=args.output, csv1=args.csv1, csv2=args.csv2, only_pipeline=None, log_scale=False)
    #Plot.time_elapsed_comparison(filename=args.output, csv1=args.csv1, csv2=args.csv2, only_pipeline=None, log_scale=True)
    #Plot.time_elapsed_comparison(filename=args.output, csv1=args.csv1, csv2=args.csv2, only_pipeline=None, log_scale=False, cap_outliers=0.06)

def plot_metrics_comparison():
    # python -m src.charts './logs/metrics/chunk_summary_best.csv' './logs/metrics/chunk_summary_worst.csv' './logs/metrics/chunk_summary_llm.csv' --output='./logs/charts/metrics_comparison.png'

    import argparse

    parser = argparse.ArgumentParser(description='Compare metrics from three CSV files')
    parser.add_argument('csv1', help='Path to first CSV file')
    parser.add_argument('csv2', help='Path to second CSV file')
    parser.add_argument('csv3', help='Path to third CSV file')
    parser.add_argument('--output', default='./logs/charts/runtime_comparison.png', help='Output filename for chart')

    args = parser.parse_args()

    Plot.summary_comparison(filename=args.output, paths=[args.csv1, args.csv2, args.csv3], fixed_colors=["tab:blue", "tab:orange", "tab:green"], labels=["Best", "Fast", "LLM-Only"])


if __name__ == "__main__":
    # plot_time_comparison()
    plot_metrics_comparison()
