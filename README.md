# 🏏 Cricket Analytics Dashboard

An interactive **Cricket Analytics Dashboard** built using **Python, Streamlit, and REST APIs** to analyze player performance, rankings, and match insights across multiple cricket formats.

This project integrates live cricket data from external APIs, processes it through a custom data pipeline, and presents meaningful analytics through an intuitive web interface.

---

## 🚀 Features

* 📊 Player performance analysis (batting & bowling)
* 🏆 Rankings and comparative insights
* 🔎 Query-based analytics engine
* 🌍 Multi-format support (ODI, T20, Test)
* ⚡ Real-time data integration via API
* 🖥️ Interactive dashboard using Streamlit
* 🗄️ Data processing pipeline with modular architecture
* 📁 Local data storage for reproducibility

---

## 🧠 Project Architecture

```
User Interface (Streamlit)
        │
        ▼
Analytics Agent (Query Logic)
        │
        ▼
Data Pipeline (API Fetch + Processing)
        │
        ▼
External Cricket API / Local Data
```

---

## 🛠️ Tech Stack

**Programming Language**

* Python

**Libraries & Tools**

* Streamlit
* Pandas
* Requests
* Python-Dotenv
* PostgreSQL (optional integration)
* Git & GitHub

**Concepts Used**

* REST API Integration
* Data Cleaning & Transformation
* Modular Pipeline Design
* Environment Variable Management
* Interactive Data Visualization

---

## 📂 Project Structure

```
CRICBUZZAPP_RAMASEKAR/
│── cricbuzzapp.py          # Streamlit application
│── pipeline.py             # Data fetching & processing logic
│── requirements.txt
│── .env.example            # Environment variable template
│
├── data/                   # Sample datasets (CSV/JSON)
└── docs/                   # Project documentation
```

---

## ⚙️ Installation & Setup

### 1️⃣ Clone Repository

```
git clone https://github.com/rlalmla/cricket-analytics-dashboard.git
cd cricket-analytics-dashboard
```

### 2️⃣ Create Virtual Environment (Recommended)

```
python -m venv venv
venv\Scripts\activate
```

### 3️⃣ Install Dependencies

```
pip install -r requirements.txt
```

### 4️⃣ Configure Environment Variables

Create a `.env` file using `.env.example` as reference:

```
RAPIDAPI_KEY=your_api_key_here
DB_HOST=localhost
DB_PORT=5432
DB_NAME=cricbuzz
DB_USER=your_username
DB_PASSWORD=your_password
```

### 5️⃣ Run Application

```
streamlit run cricbuzzapp.py
```

---

## 📸 Screenshots

Example:

## 📸 Screenshots

### Matches Dashboard
![Matches Dashboard](https://github.com/user-attachments/assets/ca8f15e9-faf8-41f6-afa9-0b67210c0735)

### Player Statistics
![Player Statistics](https://github.com/user-attachments/assets/de497c16-3eea-4370-a052-c44c25122005)

### SQL Analytics
![SQL Analytics](https://github.com/user-attachments/assets/f2a0eae9-5e24-4973-a61a-a333dd070fef)

### CRUD Operations
![CRUD Operations](https://github.com/user-attachments/assets/6bffd6bb-1319-4763-872e-55d4d327b694)


## 🎯 Key Learning Outcomes

* Building end-to-end data applications
* API data extraction and transformation
* Designing modular analytics pipelines
* Developing interactive dashboards
* Managing configuration securely with environment variables
* Version control with Git

---

## 🔮 Future Enhancements

* Advanced predictive analytics (match outcome prediction)
* Deployment on cloud platforms (AWS / Streamlit Cloud)
* Database optimization and caching
* Enhanced visualizations with Plotly
* User authentication system

---

## 👨‍💻 Author

**Rama Sekar**

GitHub: https://github.com/rlalmla

---

## 📜 License

This project is for educational and academic purposes.
