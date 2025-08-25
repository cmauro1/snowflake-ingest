# snowflake-ingest
<h1>Data ingestion into Snowflake</h1>

<h2>A Quick Walkthrough</h2>

<h3>Dependencies</h3>
<ul>
  Python 3.10 (other versions may cause dependency issues with underlying packages)
  Java JDK 11 or 17
</ul>

<h3>Environment Setup</h3>
<body>
First, start with creating your Python virtual environment.
  
  From your command line, navigate to your project directory.
  If Python 3.10 is in your PATH variable, execute the following command:
    python -m venv \sf-stage-venv
  If Python 3.10 is NOT in your PATH variable, you will have to invoke the full path to that version:
    <path\to\Python\3.10\python.exe> -m venv \sf-stage-venv
  Now activate your virtual environment with this command:
    sf-stage-venv\Scripts\activate

Next, you will pip install the required Python libraries from requirements.txt.
  Run the command:
    <path\to\Python\3.10\python.exe> -m pip install requirements.txt
  
</body>

<h3>Create Database Objects</h3>

<h3>Ingest via Python Script</h3>
