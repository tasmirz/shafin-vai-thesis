#!/usr/bin/env python3
import tkinter as tk
from tkinter import ttk, scrolledtext
import subprocess
import threading
import os
from pathlib import Path
import json

class BenchmarkGUI(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("PTD-BenchLab Master")
        self.geometry("900x700")

        self.root_dir = Path(__file__).resolve().parents[2]
        self.datasets = self.load_datasets()

        # UI Setup
        self.create_widgets()

    def load_datasets(self):
        dataset_dir = self.root_dir / "datasets-curated"
        datasets = []
        if dataset_dir.exists():
            for f in dataset_dir.glob("*.csv"):
                datasets.append(f.name)
        return sorted(datasets) if datasets else ["No datasets found"]

    def create_widgets(self):
        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)

        # Tab 1: Single Run
        tab1 = ttk.Frame(notebook)
        notebook.add(tab1, text="Single Run")
        self.build_single_run_tab(tab1)

        # Tab 2: Dataset Ablation
        tab2 = ttk.Frame(notebook)
        notebook.add(tab2, text="Dataset Ablation (All Algos)")
        self.build_ablation_tab(tab2)

        # Tab 3: Full Evaluation
        tab3 = ttk.Frame(notebook)
        notebook.add(tab3, text="Full Evaluation")
        self.build_full_eval_tab(tab3)

        # Common Output Area
        output_frame = ttk.LabelFrame(self, text="Console Output")
        output_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        self.output_text = scrolledtext.ScrolledText(output_frame, height=15, state=tk.DISABLED, bg="black", fg="lightgray", font=("Consolas", 10))
        self.output_text.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

    def build_single_run_tab(self, parent):
        frame = ttk.Frame(parent, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Engine:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.single_engine = ttk.Combobox(frame, values=["apache-spark", "hadoop"], state="readonly")
        self.single_engine.current(0)
        self.single_engine.grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="Algorithm:").grid(row=1, column=0, sticky=tk.W, pady=5)
        algos = ["baseline", "dscp-only", "aes-only", "aes-dscp", "improved-baseline", "improved-dscp-only", "improved-aes-only", "improved-aes-dscp"]
        self.single_algo = ttk.Combobox(frame, values=algos, state="readonly")
        self.single_algo.current(0)
        self.single_algo.grid(row=1, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="Dataset:").grid(row=2, column=0, sticky=tk.W, pady=5)
        self.single_dataset = ttk.Combobox(frame, values=self.datasets, state="readonly", width=40)
        self.single_dataset.current(0)
        self.single_dataset.grid(row=2, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="K:").grid(row=3, column=0, sticky=tk.W, pady=5)
        self.single_k = ttk.Entry(frame, width=10)
        self.single_k.insert(0, "10")
        self.single_k.grid(row=3, column=1, sticky=tk.W, pady=5)

        ttk.Label(frame, text="Partitions:").grid(row=4, column=0, sticky=tk.W, pady=5)
        self.single_partitions = ttk.Entry(frame, width=10)
        self.single_partitions.insert(0, "8")
        self.single_partitions.grid(row=4, column=1, sticky=tk.W, pady=5)

        self.single_validate = tk.BooleanVar(value=True)
        ttk.Checkbutton(frame, text="Validate Exact (Oracle)", variable=self.single_validate).grid(row=5, column=0, columnspan=2, sticky=tk.W, pady=5)

        ttk.Button(frame, text="Run Benchmark", command=self.run_single).grid(row=6, column=0, columnspan=2, pady=15)

    def build_ablation_tab(self, parent):
        frame = ttk.Frame(parent, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)
        
        ttk.Label(frame, text="Select Dataset Profile:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.ablation_profile = ttk.Combobox(frame, values=["smartphone", "road-smoke", "road-full", "road-full-20q"], state="readonly")
        self.ablation_profile.current(1)
        self.ablation_profile.grid(row=0, column=1, sticky=tk.W, pady=5)
        
        ttk.Button(frame, text="Run Ablation Suite (Spark)", command=self.run_ablation).grid(row=1, column=0, columnspan=2, pady=15)

    def build_full_eval_tab(self, parent):
        frame = ttk.Frame(parent, padding=10)
        frame.pack(fill=tk.BOTH, expand=True)

        ttk.Label(frame, text="Select Dataset Profile:").grid(row=0, column=0, sticky=tk.W, pady=5)
        self.full_profile = ttk.Combobox(frame, values=["smartphone", "road-smoke", "road-full", "road-full-20q"], state="readonly")
        self.full_profile.current(1)
        self.full_profile.grid(row=0, column=1, sticky=tk.W, pady=5)

        ttk.Button(frame, text="Run Full Comparison Suite (Spark & Hadoop)", command=self.run_full).grid(row=1, column=0, columnspan=2, pady=15)

    def log(self, text):
        self.output_text.config(state=tk.NORMAL)
        self.output_text.insert(tk.END, text + "\n")
        self.output_text.see(tk.END)
        self.output_text.config(state=tk.DISABLED)
        self.update_idletasks()

    def run_subprocess(self, cmd, env=None):
        self.log(f"> Running command: {' '.join(cmd)}")
        def target():
            env_vars = os.environ.copy()
            if env:
                env_vars.update(env)
            
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, cwd=str(self.root_dir), env=env_vars)
            
            for line in iter(process.stdout.readline, ""):
                if line:
                    self.log(line.strip("\n"))
            process.stdout.close()
            process.wait()
            self.log(f"> Command finished with code {process.returncode}")

        threading.Thread(target=target, daemon=True).start()

    def run_single(self):
        engine = self.single_engine.get()
        algo = self.single_algo.get()
        dataset = self.single_dataset.get()
        csv_path = str(self.root_dir / "datasets-curated" / dataset)
        
        env = {
            "ALGORITHM": algo,
            "CSV_PATH": csv_path,
            "K": self.single_k.get(),
            "PARTITIONS": self.single_partitions.get(),
            "VALIDATE_EXACT": "true" if self.single_validate.get() else "false"
        }
        
        if engine == "apache-spark":
            script = "scripts/research/run_csv_benchmark.sh" if not algo.startswith("improved-") else "scripts/research/run_improved_csv_benchmark.sh"
        else:
            script = "scripts/research/run_hadoop_csv_benchmark.sh"
            
        self.run_subprocess(["bash", script], env)

    def run_ablation(self):
        env = {"PROFILE": self.ablation_profile.get()}
        self.run_subprocess(["bash", "scripts/research/run_ablation_suite.sh"], env)

    def run_full(self):
        env = {"PROFILE": self.full_profile.get()}
        self.run_subprocess(["bash", "scripts/research/run_full_comparison_suite.sh"], env)

if __name__ == "__main__":
    app = BenchmarkGUI()
    app.mainloop()
