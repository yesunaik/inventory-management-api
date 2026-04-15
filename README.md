# 📦 Inventory Management API

## 🚀 Overview

A production-grade Inventory Management System built using FastAPI, designed for real-time tracking of items, products, stock movement, and supply chain operations.

This system supports complete inventory lifecycle including purchase, issue, return, and stock monitoring with scalable backend architecture.

---

## ⚙️ Features

* Item & Product Management
* Purchase & Return Handling
* Stock Tracking & Inventory Control
* Site & Location Management
* Secure API with Token Authentication
* File Upload Support (S3 Integration)
* Modular & Scalable Architecture

---

## 🛠️ Tech Stack

* **Backend:** Python, FastAPI
* **Database:** MySQL (mysql-connector)
* **Architecture:** Modular (Routes, Services, DB Layer)
* **Authentication:** Token-based security
* **Storage:** AWS S3 (file handling)
* **Tools:** Uvicorn, Pydantic

---

## 📁 Project Structure

```
app/
 ├── core/        # Security & authentication
 ├── db/          # Database connection
 ├── routes/      # API endpoints
 ├── services/    # Business logic
 ├── schemas/     # Request/Response models
 ├── utils/       # Helper functions
 └── main.py      # Application entry point
```

---

## ▶️ How to Run

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Run the server

```bash
uvicorn app.main:app --reload
```

### 3. Access API Docs

```
http://127.0.0.1:8000/docs
```

---

## 🔐 Security

* Token-based authentication implemented
* Protected API routes using middleware

---

## 📈 Key Highlights

* Designed scalable backend using service-based architecture
* Handles complex inventory workflows (purchase → stock → issue → return)
* Clean separation of concerns for maintainability
* Production-ready structure used in real-world systems

---

## 👨‍💻 Author

**Yesu Naik**
Software Engineer | Python | IoT | Embedded Systems

📧 [yesunaik2001@gmail.com](mailto:yesunaik2001@gmail.com)
🔗 LinkedIn: https://www.linkedin.com/in/yesu-naik-749734246/
