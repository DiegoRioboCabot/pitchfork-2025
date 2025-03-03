# 🎵 pitchfork-2025

*A blazing-fast, multithreaded scraper for Pitchfork's album reviews, inspired by [Nolan Conaway's](https://github.com/nolanbconaway) work from [2022](https://github.com/nolanbconaway/pitchfork-2022) and [2018](https://github.com/nolanbconaway/pitchfork-data).*

## 🚀 Key Features
- **📊 Ready-to-Use SQL Views** for easy analysis & visualization
- **⚡ 63 minutes** to scrape **Pitchfork's sitemap + album reviews + author bios**  
  *(Benchmarked on a 24-thread CPU with a 200 Mbps internet connection)*
- **🔗 SQLite Database with 5NF Normalization** *(except for `author_type_evolution` in 4NF)*
- **📂 Modular & Extendable** – Well-organized code for future improvements
- **💡 Multithreaded Performance** – Scrapes multiple pages simultaneously
- **🔍 JSON-based Parsing**

---

## 🛠️ How It Works
### 🔗 URL Mapping
1. Scrapes **Pitchfork's sitemap** to extract the following types of URLs:
   - 📰 News (*'/news/'*)
   - 🏆 Live Grammy coverage (*'/grammys/'*)
   - 🎵 Album reviews (*'/reviews/albums/'*)
   - 🎧 Track reviews (*'/reviews/tracks/'*)
   - 🎬 Movie reviews (*'/thepitch/'*)
   - ✍️ Features (*'/features/'*)
   - 🎥 Videos (*'/tv/'*)
   
2. Scrapes **album review pages** to extract:
   - 🎤 *Artists* (Links to their dedicated pages)
   - ✍️ *Authors* (Links to their bio pages)

3. Scrapes **author pages** for **detailed author metadata**.

---

## ⚙️ Scraping & Parsing

This script uses **BeautifulSoup** & **JSON parsing** allowing to:
* ❌ avoid unicode characters 
* 🕵️ reveal ***hidden* data structures**.
* 📒 Apply **data normalization techniques** to ensure minimal redundancy.

### **🎵 Album Data Extraction
- Avoids messy **Unicode character cleaning** by **extracting structured JSON data**.
- Navigates **deeply nested JSON structures** to retrieve key data fields.

### ✍️ Author Data Extraction
- Retrieves **author roles & biography**.

---

## 📦 Database & Storage
- **🔗 SQLite database** stores all scraped data.
- **⚡ Optimized with indexes & foreign keys** for fast queries.
- **📊 SQL Views** allow users to start analyzing data **without writing complex SQL**.

### **📂 Database Schema
```mermaid
erDiagram
    entities {
        INTEGER entity_id PK
        TEXT entity}

    genres {
        INTEGER genre_id PK
        TEXT genre}

    keywords {
        INTEGER keyword_id PK
        TEXT keyword}

    labels {
        INTEGER label_id PK
        TEXT label}

    urls {
        INTEGER url_id PK
        TEXT url
        INTEGER year
        INTEGER month
        INTEGER week
        INTEGER is_review
        INTEGER is_album
        INTEGER is_author
        INTEGER is_artist}

    albums {
        TEXT album_id PK
        TEXT album
        TEXT publisher
        INTEGER release_year
        INTEGER pitchfork_score
        INTEGER is_best_new_music
        INTEGER is_best_new_reissue}

    artists {
        INTEGER artist_id PK
        TEXT artist
        TEXT url_id FK}

    authors {
        TEXT author_id PK
        TEXT author
        TEXT url_id FK}

    author_bios {
        TEXT author_id FK
        TEXT date_pub
        INTEGER revisions
        TEXT bio}

    author_types {
        INTEGER author_type_id PK
        TEXT author_type}

    author_type_evolution {
        TEXT author_id FK
        INTEGER author_type1_id FK
        INTEGER author_type2_id FK
        TEXT as_of_date}

    reviews {
        TEXT review_id PK
        INTEGER revisions
        TEXT url_id FK
        TEXT body
        TEXT description
        TEXT date_pub
        TEXT date_mod}

    review_albums {
        TEXT review_id FK
        TEXT album_id FK}

    review_labels {
        TEXT review_id FK
        INTEGER label_id FK}

    review_artists {
        TEXT review_id FK
        INTEGER artist_id FK}

    review_authors {
        TEXT review_id FK
        TEXT author_id FK}

    review_keywords {
        TEXT review_id FK
        INTEGER keyword_id FK
        REAL score}

    review_entities {
        TEXT review_id FK
        INTEGER entity_id FK
        REAL score}

    review_artist_genres {
        TEXT review_id FK
        INTEGER artist_id FK
        INTEGER genre_id FK}

    scraping_events {
        TEXT timestamp PK
        INTEGER url_id FK
        TEXT process
        INTEGER success
        TEXT message}

    %% Relationships
    albums ||--o{ review_albums : contains
    labels ||--o{ review_labels : associated_with
    artists ||--o{ review_artists : performed_by
    authors ||--o{ review_authors : written_by
    keywords ||--o{ review_keywords : contains
    entities ||--o{ review_entities : references
    genres ||--o{ review_artist_genres : categorized_as
    urls ||--o{ reviews : published_on
    urls ||--o{ artists : linked_to
    urls ||--o{ authors : linked_to
    urls ||--o{ scraping_events : logged_in
    author_types ||--o{ author_type_evolution : evolves_from

```

---

## 🏗️ Project Structure
📂 **pitchfork-2025/**  
├── 📁 **scraper/** # Core scraping & data parsing logic  
│ ├── **init.py**  
│ ├── **album.py**  # Scrapes album reviews  
│ ├── **author.py** # Scrapes author bios & roles  
│ ├── **db.py** # Handles SQLite database interactions  
│ ├── **general.py** # Helper functions & utilities  
│ ├── **globals.py** # Global variables for multithreading  
│ ├── **sitemap.py** # Handles sitemap extraction  
│ ├── **types.py** # Custom data types & NamedTuples to communicate with the database  
│ └── **README.md** # Documentation for the scraper  
│ ├── 📄 **Scrape_Pitchfork.py** # Main script to execute scraping  
├── 📂 **data/** # Stores scraped SQLite database  
├── 📂 **sql_scripts/**  
| ├── ...# SQL script to create the database **indexes**, **views** and **metadata** 
└── 📄 README.md # Project documentation (this file) 

---

## 🚀 Getting Started
### 🔧 Installation
1. **Clone the repository**
```bash
git clone https://github.com/DiegoRioboCabot/pitchfork-2025.git
cd pitchfork-2025
```
2. Install dependencies
```bash 
pip install -r requirements.txt
```

### 🔥 Running the Scraper
Just run the following code on a terminal:  
```bash 
python Scrape_Pitchfork.py
```

### 💾 SQLite Database
After running the script, your data will be in stored inside ```data/``` 
and the filename is ```Pithfrok_Album_Reviews_yyyy_mm_dd.db``` where  
* *yyyy* represent the year in which the script started, 
* *mm* the month and 
* *dd* the day.

## 📝 Contributing
Feel free to fork the repo and submit improvements.

## 📜 License
This project is licensed under the *GNU 3 General Public* License.