# RealTime-GPS-Streaming
Database: https://www.microsoft.com/en-us/download/details.aspx?id=52367

# Environment Preparation
    Step 1: Set "System variables" for SPARK: Create Variable "SPARK_HOME" VALUE "<path>\RealTime-GPS-Streaming\.venv\Lib\site-packages\spark\jars\spark-3.5.7-bin-hadoop3"
    Step 2: Set "System variables" for HADOOP: Create Variable "HADOOP_HOME" VALUE "<path>\RealTime-GPS-Streaming\.venv\Lib\site-packages\hadoop"
    Step 3: Set "Path" in "System variables": %HADOOP_HOME%\BIN\WINUTILS.EXE
                        %HADOOP_HOME%\bin
                        %SPARK_HOME%\bin

# Data Preparation
    Step 1: Run "Convert2CSV.py" # To mereg the data as ".plt" in "data_plt" folder of all user in "merged_points.csv" file.
    Step 2: Run "data_preparation.py" # To prepare data for each user in "data_csv" folder.

# Processing
    Step 1: Activate the .venv (Python version 3.13.9)
        If using CMD: 
            Run: .\.venv\Scripts\activate
        If using Powershell:
            Run: .\.venv\Scripts\Activate.ps1

    Step 2: Start Dokcer Application and Run "Docker Compose"
        Run: docker compose -f "./docker-compose/docker-compose.yml"  up -d --build

    Step 3:  Star Producer
        Run: .\src\Producer.py

    Step 4: Start Consumer
        Run: .\src\Consumer.py

    Step 5: Start Grafana to visualize
        Open: http://localhost:3000/