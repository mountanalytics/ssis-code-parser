# SSIS Code Parser

This project is an **SSIS (SQL Server Integration Services) DTSX** code parser that analyzes DTSX package files, visualizes the data flows using **Sankey Diagrams** accessible through a dasboard generated with **Flask** and **Plotly**, and automatically generates a **Word (docx)** report summarizing the content of the DTSX file, their complexity and other characteristics.

This project only handles SSIS files with **ExecutableType: Microsoft.Package** and **PackageFormatVersion:8**.

## Features

- **DTSX Parsing**: Extracts and analyzes components from the DTSX package, including data flow, control flow, and transformations.
- **Sankey Diagram Visualization**: Visualizes the flow of tasks and data using a Sankey diagram for easy understanding of data movement and control flow.
- **Auto-Generated Documentation**: Creates a structured Word report (docx) summarizing the DTSX package details, including tasks, connections, and data flow logic.
- **Web Interface**: Provides an interactive web interface built with Flask for uploading DTSX files and viewing the Sankey diagrams.

## Project Structure

### Folders

- **data**: folder with sample .dtsx files
- **output-data**: folder with the output parsed data, used to generate the sankey graph and automatic reports
- **modules**: folder with the python scripts that handle the parsing, sankey graph dashboard and report generation

## Installation

### Prerequisites

Ensure you have **Python > 3.10** installed. You will also need the following Python libraries:

- `Flask`
- `Plotly`
- `xmltodict` (for XML parsing of DTSX files)
- `python-docx` (for report generation)
- `Pandas`
- `Sqlglot` (https://github.com/KaiserM11/sqlglot_ma_Transformations)


## Usage

1. Clone the repository: 

```bash
git clone https://github.com/mountanalytics/ssis-code-parser.git  
cd ssis-code-parser
```

2. You can install the required packages by running:

```bash
pip install -r requirements.txt
```

3. Access the main.py file, input the path to your .dtsx file, and run the python script

4. Access the dashboard through

```bash
http://127.0.0.1:5000
```







