import matplotlib.pyplot as plt
import os
import pandas as pd
import seaborn as sns
from datetime.datetime import now
from src.util import Log, get_merged_df
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
            # Clean function names to ensure matching works
            df['function'] = df['function'].astype(str).str.replace(r'\[.*?\]$', '', regex=True).str.strip()

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
        plt.savefig(filename, bbox_inches='tight')
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

    @staticmethod
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
            if value <= 0:
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


    @staticmethod
    def save_metrics_csv(
        metrics: Dict[str, float],
        run_id: Optional[str] = None,
        filename: str = "./logs/chunk_scores.csv"
    ) -> None:
        """Save metrics as a single row, appending to existing CSV.
        @details
        Row-major format: each metric is a column, each run is a row.
        Uses timestamp as run_id if not provided.
        """
        if run_id is None:
            run_id = now().isoformat()
        
        # Single-row DataFrame with run_id as first column
        row_data = {"run_id": run_id, **metrics}
        current_df = pd.DataFrame([row_data])
        
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        
        merged_df = get_merged_df(current_df, filename, run_id)
        merged_df.to_csv(filename, index=False)
        
        Log.chart_message(prefix=Log.ch_dump, msg=Log.msg_chart_saved("Saved summary metrics CSV", filename))

    METRIC_GROUPS = {
        "SOURCE SIMILARITY": ["bertscore", "rougeL_recall", "jsd_stats", "ncd_overlap", "novel_ngrams"],
        "FACTUALITY": ["salience_recall", "entity_coverage", "entity_hallucination", "questeval", "nli_faithfulness"],
        "NARRATIVE FLOW": ["readability_delta", "lexical_diversity", "stopword_ratio", "entity_grid_coherence", "sentence_coherence", "bookscore"],
    }


    @staticmethod
    def summary_comparison(
        filename: str,
        paths: list[str],
        fixed_colors: list[str],
        labels: list[str]
    ) -> None:
        """Compare metrics across CSV files, aggregating chunk rows per file.
        
        Expects row-major CSVs: run_id, metric1, metric2, ...
        Each file becomes one bar group (mean across all rows).
        """
        import matplotlib.pyplot as plt
        import numpy as np

        metric_cols = None
        aggregated = []  # one {metric: mean_value} dict per file

        for path in paths:
            df = pd.read_csv(path)
            cols = [c for c in df.columns if c != "run_id"]
            
            if metric_cols is None:
                metric_cols = cols
            
            agg_dict = {m: df[m].mean() for m in cols}
            aggregated.append(agg_dict)

        # Normalize each file's metrics
        normalized = [Plot.normalize_metrics(d) for d in aggregated]

        # Build x positions with group spacing
        x_positions = []
        x_labels = []
        group_ends = []
        spacing = 0.5
        x = 0.0

        group_names = list(Plot.METRIC_GROUPS.keys())
        for group_name in group_names:
            metrics = Plot.METRIC_GROUPS[group_name]
            group_start_x = x
            for m in metrics:
                if m in metric_cols:
                    x_positions.append(x)
                    x_labels.append(Plot.METRIC_NAMES.get(m, m))
                    x += 1
            if x > group_start_x:
                group_ends.append(x - 1)
                x += spacing

        # Separator positions: midpoint between groups
        separator_xs = []
        for i in range(len(group_ends) - 1):
            separator_xs.append(group_ends[i] + spacing / 2)

        bar_width = 0.9 / len(paths)
        plt.figure(figsize=(max(12, len(x_labels) * 0.5), 6))

        # Draw bars
        for i, norm_dict in enumerate(normalized):
            label = labels[i] if i < len(labels) else os.path.splitext(os.path.basename(paths[i]))[0]
            color = fixed_colors[i] if i < len(fixed_colors) else None
            offset = (i - (len(paths) - 1) / 2) * bar_width

            values = []
            for group_name in group_names:
                for m in Plot.METRIC_GROUPS[group_name]:
                    if m in metric_cols:
                        values.append(norm_dict.get(m, 0))

            positions = [pos + offset for pos in x_positions]
            plt.bar(positions, values, width=bar_width, label=label, color=color)

        plt.xticks(x_positions, x_labels, rotation=20, ha="right")
        plt.ylabel("Score")
        title = "Quality Comparison (Chunk-Level Summary)"
        plt.title(title)
        plt.legend()

        # Separators and group labels
        group_label_y = 1.0 - 0.02
        group_label_offset = 0.5

        for i, sep_x in enumerate(separator_xs):
            plt.axvline(x=sep_x, color="gray", linestyle="dotted", linewidth=1)
            plt.text(sep_x - group_label_offset, group_label_y, group_names[i],
                     fontsize=10, fontweight="bold", ha="right", va="bottom")

        # Rightmost group label
        if group_ends:
            last_label_x = group_ends[-1] + bar_width / 2
            plt.text(last_label_x, group_label_y, group_names[-1],
                     fontsize=10, fontweight="bold", ha="right", va="bottom")

        plt.tight_layout()
        os.makedirs(os.path.dirname(filename), exist_ok=True)
        plt.savefig(filename, bbox_inches="tight")
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

    #Plot.time_elapsed_comparison(filename=args.output, csv1=args.csv1, csv2=args.csv2, only_pipeline=False, log_scale=False)
    #Plot.time_elapsed_comparison(filename=args.output, csv1=args.csv1, csv2=args.csv2, only_pipeline=False, log_scale=True)
    Plot.time_elapsed_comparison(filename=args.output, csv1=args.csv1, csv2=args.csv2, only_pipeline=False, log_scale=False, cap_outliers=0.06)

def plot_metrics_comparison():
    # python -m src.charts './logs/results/chunk_18/chunk_summary_best.csv' './logs/results/chunk_18/chunk_summary_fast.csv' './logs/results/chunk_18/chunk_summary_llm.csv' --output='./logs/charts/chunk_18/metrics_comparison.png'
    # python -m src.charts './logs/results/chunk_summary_best_3x.csv' './logs/results/chunk_summary_fast_3x.csv' './logs/results/chunk_summary_llm_3x.csv' --output='./logs/charts/metrics_comparison_3x.png'
    # .pdf for higher quality
    import argparse

    parser = argparse.ArgumentParser(description='Compare metrics from three CSV files')
    parser.add_argument('csv1', help='Path to first CSV file')
    parser.add_argument('csv2', help='Path to second CSV file')
    parser.add_argument('csv3', help='Path to third CSV file')
    parser.add_argument('--output', default='./logs/charts/runtime_comparison.png', help='Output filename for chart')

    args = parser.parse_args()

    Plot.summary_comparison(filename=args.output, paths=[args.csv1, args.csv2, args.csv3], fixed_colors=["tab:blue", "tab:orange", "tab:green"], labels=["Best", "Fast", "LLM-Only"])


if __name__ == "__main__":
    #plot_time_comparison()
    plot_metrics_comparison()
