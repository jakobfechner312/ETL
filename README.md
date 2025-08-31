### ETL Pipelines (static + adaptive)

This repository contains two data processing pipelines:
- **static_pipeline**: A deterministic ETL process for cleaning and merging data.
- **adaptive**: An LLM-assisted pipeline for more complex data cleaning tasks, which requires an OpenAI API key.

---

## Linux Guide

#### 1. Prerequisites (for Debian/Ubuntu-based systems)
This command ensures you have the necessary tools to create virtual environments and install packages.
```bash
sudo apt-get update && sudo apt-get install -y python3-venv python3-pip
```

#### 2. Setup
This block creates a virtual environment, activates it, installs all required Python packages, and creates a `.env` file for your API key.
```bash
python3 -m venv .venv
source .venv/bin/activate
python3 -m pip install -r requirements.txt
cp .env.example .env
```
After setup, edit the `.env` file to add your `OPENAI_API_KEY`.

#### 3. Running the Pipelines
Once the setup is complete and your environment is active, you can run the pipelines.

**Static Pipeline**
```bash
python3 static_pipeline/main_pipeline.py
```
> Outputs are saved to `static_pipeline/data/processed/`.

**Adaptive Pipeline (Jupyter Notebook)**

First, install a dedicated Jupyter kernel for your environment.
```bash
python3 -m ipykernel install --user --name etl-linux --display-name "ETL Pipeline (Linux)"
```
Then, start the notebook.
```bash
jupyter notebook adaptive/dynamic_main.ipynb
```
> In Jupyter, select the "ETL Pipeline (Linux)" kernel.

---

## Windows Guide

#### 1. Setup
This block creates a virtual environment, activates it, installs all required Python packages, and creates a `.env` file for your API key.
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
copy .env.example .env
```
After setup, edit the `.env` file to add your `OPENAI_API_KEY`.

#### 2. Running the Pipelines
Once the setup is complete and your environment is active, you can run the pipelines.

**Static Pipeline**
```powershell
python static_pipeline/main_pipeline.py
```
> Outputs are saved to `static_pipeline/data/processed/`.

**Adaptive Pipeline (Jupyter Notebook)**

First, install a dedicated Jupyter kernel for your environment.
```powershell
python -m ipykernel install --user --name etl-windows --display-name "ETL Pipeline (Windows)"
```
Then, start the notebook.
```powershell
jupyter notebook adaptive/dynamic_main.ipynb
```
> In Jupyter, select the "ETL Pipeline (Windows)" kernel.

