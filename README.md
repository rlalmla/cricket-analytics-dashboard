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

```<img width="1886" height="867" alt="Screenshot 2026-02-22 121725" src="https://github.com/user-attachments/assets/52c3c8f0-ba8d-4c25-b4b0-c6c64df3bcd3" />
<img width="1902" height="872" alt="Screenshot 2026-02-22 122030" src="https://github.com/user-attachments/assets/c9b6c2c0-2059-48a2-af83-2e33ab960672" />
<img width="1913" height="864" alt="Screenshot 2026-02-22 122054" src="https://github.com/user-attachments/assets/731ae3fb-3053-4679-9c97-c5e9b92803aa" />
<img width="1901" height="850" alt="Screenshot 2026-02-22 122125" src="https://github.com/user-attachments/assets/09ae53f7-9785-401c-b89c-f810cce5d1bd" />
<img width="1899" height="851" alt="Screenshot 2026-02-22 122224" src="https://github.com/user-attachments/assets/1eb4b0f1-0ce4-408c-9db1-de79c97700ee" />

![Dashboard](assets/dashboard.png
```

---

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
