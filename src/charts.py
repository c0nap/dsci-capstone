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
        # Read data from CSV files
        if csv1 and csv2:
            df1 = pd.read_csv(csv1)
            df2 = pd.read_csv(csv2)
        else:
            raise ValueError("Both csv1 and csv2 must be provided")

        # Process both datasets
        def process_df(df: pd.DataFrame, filter_mode: Optional[bool]) -> pd.DataFrame:
            # 1. Filter if requested
            if filter_mode is True:
                df = df[df['function'].str.contains('pipeline', case=False, na=False)]
            elif filter_mode is False:
                df = df[~df['function'].str.contains('pipeline', case=False, na=False)]
            # If filter_mode is None, we skip filtering but proceed to aggregation
            
            # 2. Aggregate (Group by function)
            # Check if run_id exists to allow multi-level aggregation, otherwise direct group
            if 'run_id' in df.columns:
                per_run_avg = df.groupby(['run_id', 'function'])['elapsed'].mean().reset_index()
                return per_run_avg.groupby('function')['elapsed'].mean().reset_index()
            else:
                return df.groupby('function')['elapsed'].mean().reset_index()

        df1 = process_df(df1, only_pipeline)
        df2 = process_df(df2, only_pipeline)

        # Merge on function names to align bars
        merged = pd.merge(df1, df2, on='function', suffixes=('_left', '_right'))
        merged = merged.sort_values(by=['function'], ascending=False).reset_index(drop=True)

        if merged.empty:
            print("Warning: No matching functions found between datasets.")
            return

        # Create figure
        height = max(4, len(merged) * 0.4) # Ensure minimum height
        fig, ax = plt.subplots(figsize=(10, height))

        # Plot bars going inward from center
        y_pos = range(len(merged))
        ax.barh(y_pos, -merged['elapsed_left'], align='center', label='Best', color='tab:blue')
        ax.barh(y_pos, merged['elapsed_right'], align='center', label='Fast', color='tab:orange')

        # Configure axes with log scale to handle outliers
        if log_scale:
            ax.set_xscale('symlog', linthresh=1.0)
        
        if cap_outliers > 0:
            # Calculate limits based on aggregated data
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
        "bertscore" : "BERTScore (F1)",
        "novel_ngrams" : "Repeated N-Grams",
        "jsd_stats" : "JSD Alignment",
        "entity_coverage" : "Entity Coverage",
        "entity_hallucination" : "Lack of Hallucination",
        "ncd_overlap" : "NCD Overlap",
        "salience_recall" : "Salience Recall",
        "nli_faithfulness" : "Entailment Faithfulness",
        "readability_delta" : "Readability Delta",
        "sentence_coherence" : "Embedding Coherence",
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
            elif key == "bertscore":
                # Clamp to [0, 1] range
                normalized = max(0.0, min(1.0, value))
                metrics[key] = normalized
            elif key in ["jsd_stats", "novel_ngrams", "ncd_overlap", "entity_hallucination"]:
                metrics[key] = 1 - value
            value = metrics[key]
            if value == 0:
                metrics[key] = 0.01
        return metrics

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



    METRIC_GROUPS = {
        "SOURCE SIMILARITY": ["bertscore", "rougeL_recall", "jsd_stats", "ncd_overlap", "novel_ngrams"],
        "FACTUALITY": ["salience_recall", "entity_coverage", "entity_hallucination", "questeval", "nli_faithfulness"],
        "NARRATIVE FLOW": ["readability_delta", "lexical_diversity", "stopword_ratio", "entity_grid_coherence", "sentence_coherence", "bookscore"],
    }


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
                # Create a dict keyed by original metric names
                metric_to_val = dict(zip(merged["metric"], merged[col]))
                normalized = Plot.normalize_metrics(metric_to_val)
                # Re-align the normalized values back to the DataFrame column
                merged[col] = merged["metric"].map(normalized)

        # Then rename metrics for display
        merged["metric"] = merged["metric"].apply(lambda k: Plot.METRIC_NAMES.get(k, k))

        # Compute x positions and spacing for groups
        x_positions = []
        x_labels = []
        group_ticks = []
        spacing = 0.5  # extra space between groups
        x = 0
        for group_name, metrics in Plot.METRIC_GROUPS.items():
            group_indices = []
            for m in metrics:
                idx = merged.index[merged["metric"] == Plot.METRIC_NAMES.get(m, m)].tolist()
                if idx:
                    x_positions.append(x)
                    x_labels.append(merged.loc[idx[0], "metric"])
                    group_indices.append(x)
                    x += 1
            if group_indices:
                group_center = np.mean(group_indices)
                group_ticks.append((group_center, group_name))
                x += spacing  # add extra space after group

        bar_width = 0.9 / len(labels)

        plt.figure(figsize=(max(12, len(x_labels)*0.5), 6))

        # Draw vertical bars
        for i, label in enumerate(labels):
            color = fixed_colors[i] if i < len(fixed_colors) else None
            offset = (i - (len(labels)-1)/2) * bar_width
            values = [merged.loc[merged["metric"] == lbl, label].values[0] for lbl in x_labels]
            plt.bar([pos + offset for pos in x_positions], values, width=bar_width, label=label, color=color)

        plt.xticks(x_positions, x_labels, rotation=20, ha="right")  # slightly tilted
        plt.ylabel("Score")
        title = "Quality Comparison (Chunk-Level Summary)"
        plt.title(title)
        plt.legend()

        # Manually define the x positions for the group separators
        group_lines_x = [4.75, 10.25]  # example positions between groups
        group_label_y = max(merged[labels].max().max(), 1) - 0.02  # vertical position for headers

        # Draw dotted lines
        for x in group_lines_x:
            plt.axvline(x=x, color="gray", linestyle="dotted", linewidth=1)

        # Draw header labels
        for x, label in zip([2.25, 5.75, 12], Plot.METRIC_GROUPS.keys()):  # adjust x for label centers
            plt.text(x, group_label_y, label, fontsize=10, fontweight="bold",
                     ha="center", va="bottom")

        plt.tight_layout()
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plt.savefig(filename)
        plt.close()

        Log.chart(title, filename)







def plot_time_comparison():
    # python -m src.charts './logs/elapsed_time_best.csv' './logs/elapsed_time_worst.csv' --output='./logs/charts/runtime_comparison.png'
    # python -m src.charts './logs/results/elapsed_time_best_3x.csv' './logs/results/elapsed_time_fast_3x.csv' --output='./logs/charts/runtime_comparison_3x.png'
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
    # python -m src.charts './logs/results/chunk_summary_best_3x.csv' './logs/results/chunk_summary_fast_3x.csv' './logs/results/chunk_summary_llm_3x.csv' --output='./logs/charts/metrics_comparison_3x.png'
    import argparse

    parser = argparse.ArgumentParser(description='Compare metrics from three CSV files')
    parser.add_argument('csv1', help='Path to first CSV file')
    parser.add_argument('csv2', help='Path to second CSV file')
    parser.add_argument('csv3', help='Path to third CSV file')
    parser.add_argument('--output', default='./logs/charts/runtime_comparison.png', help='Output filename for chart')

    args = parser.parse_args()

    Plot.summary_comparison(filename=args.output, paths=[args.csv1, args.csv2, args.csv3], fixed_colors=["tab:blue", "tab:orange", "tab:green"], labels=["Best", "Fast", "LLM-Only"])


if __name__ == "__main__":
    plot_time_comparison()
    #plot_metrics_comparison()
