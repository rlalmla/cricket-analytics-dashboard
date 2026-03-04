**🏏 Cricket Analytics Dashboard**

An interactive Cricket Analytics Dashboard built using Python, Streamlit, PostgreSQL, and REST APIs to analyze player performance, rankings, and match insights across multiple cricket formats.

This project integrates live cricket data from external APIs, processes it through a modular data pipeline, stores the results in a PostgreSQL database, and generates analytical insights using SQL queries displayed through an interactive Streamlit dashboard.

---
**🚀 Features**

📊 Player performance analysis (batting & bowling)

🏆 Player rankings and comparative insights

🔎 Query-based cricket analytics

🌍 Multi-format support (ODI, T20, Test)

⚡ Live data retrieval from Cricbuzz API

🖥️ Interactive dashboard built with Streamlit

🗄️ PostgreSQL database integration

🔄 Modular data pipeline architecture

---

**📈 SQL-based analytical computations**

**🧠 Project Architecture**

```
User Interface (Streamlit Dashboard)
        │
        ▼  
SQL Analytics Layer
        │
        ▼  
PostgreSQL Database (via SQLAlchemy)
        │
        ▼      
Data Processing Pipeline (Python + Pandas)
        │
        ▼ 
Cricbuzz API (RapidAPI)

```
---

**🛠️ Tech Stack**

**Programming Language**

-> Python

**Libraries & Tools**

-> Streamlit

-> Pandas

-> NumPy

-> Requests

-> SQLAlchemy

-> psycopg2

-> python-dotenv

-> PostgreSQL

-> Git & GitHub

**Concepts Used**

-> REST API Integration

-> Data Cleaning & Transformation

-> SQL Analytics

-> Modular Pipeline Architecture

-> Environment Variable Management

-> Interactive Data Visualization

---

**📂 Project Structure**

## 📂 Project Structure

```
CRICBUZZAPP_RAMASEKAR/
│
├── cricbuzzapp.py              # Streamlit dashboard
├── pipeline.py                 # API extraction & processing pipeline
├── requirements.txt
├── .env.example
│
├── docs/
│   └── project_documentation.pdf
│
└── data/
    └── sample datasets
```


---

**⚙️ Installation & Setup**

**1️⃣ Clone Repository**

git clone https://github.com/rlalmla/cricket-analytics-dashboard.git

cd cricket-analytics-dashboard

**2️⃣ Create Virtual Environment**

python -m venv venv

venv\Scripts\activate

**3️⃣ Install Dependencies**

pip install -r requirements.txt

---

**🗄️ Database Setup**

Before running the application, create a PostgreSQL database named:

CREATE DATABASE cricbuzz;

The application will automatically create required tables using SQLAlchemy when the pipeline runs.

---

**🔐 Configure Environment Variables**

Create a .env file using .env.example as reference.


Example:

```
API_KEY=your_api_key_here
RAPIDAPI_HOST=cricbuzz-cricket.p.rapidapi.com

DB_HOST=localhost
DB_PORT=5432
DB_NAME=cricbuzz
DB_USER=your_username
DB_PASSWORD=your_password

```

---

**▶️ Run the Application**

streamlit run cricbuzzapp.py

The Streamlit dashboard will open automatically in your browser.

## 📸 Screenshots

### Matches Dashboard
![Matches Dashboard](https://github.com/user-attachments/assets/ca8f15e9-faf8-41f6-afa9-0b67210c0735)

### Player Statistics
![Player Statistics](https://github.com/user-attachments/assets/de497c16-3eea-4370-a052-c44c25122005)

### SQL Analytics
![SQL Analytics](https://github.com/user-attachments/assets/f2a0eae9-5e24-4973-a61a-a333dd070fef)

### CRUD Operations
![CRUD Operations](https://github.com/user-attachments/assets/6bffd6bb-1319-4763-872e-55d4d327b694)

---

**🎯 Key Learning Outcomes**

-> Building end-to-end data analytics applications

-> Designing API-driven data pipelines

-> Integrating PostgreSQL with Python using SQLAlchemy

-> Performing SQL-based analytics

-> Developing interactive dashboards using Streamlit

-> Managing secure configuration using environment variables

-> Version control using Git & GitHub

---

**🔮 Future Enhancements**

Predictive match outcome modeling using machine learning

Cloud deployment (AWS / Streamlit Cloud)

Query optimization and caching

Advanced visualizations using Plotly

User authentication system

---

## 👨‍💻 Author

**Rama Sekar**

GitHub: https://github.com/rlalmla

---

## 📜 License

This project is for educational and academic purposes.
