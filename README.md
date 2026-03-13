# SimpliTrain Community Intelligence Platform — Phase 1

This project is a Python-based monitoring system that watches multiple platforms (Reddit, Quora, industry forums, web search, and Stack Overflow) for LMS-related conversations and stores matching results in a Neon PostgreSQL database.

## What it does

The `monitor.py` script aggregates content from various sources, filters them precisely using a predefined set of Tier 1 (High priority) and Tier 2 (Standard priority) keywords, and saves the matching conversations in a PostgreSQL database for analysis. It is designed to run automatically via GitHub Actions every 30 minutes.

- **Reddit:** Monitors specific subreddits (r/elearning, r/lms, etc.) and search queries via RSS.
- **Serper API:** Pulls Google search results across the web focusing on the last 24 hours.
- **Quora:** Pulls from LMS and E-learning topic RSS feeds.
- **Industry Forums:** Watches `elearningindustry.com` and `trainingindustry.com`.
- **Stack Overflow:** Pulls recent questions tagged with `scorm` and `xapi`.

## How to run locally

1. **Clone the repository:**
   ```bash
   git clone <your-repo-url>
   cd Simplitrainintelligence
   ```

2. **Set up a virtual environment:**
   ```bash
   python3 -m venv venv
   source venv/bin/activate
   ```

3. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Environment Variables:**
   Copy the `.env.example` file to `.env`:
   ```bash
   cp .env.example .env
   ```
   Modify `.env` and fill in your actual credentials:
   - `DATABASE_URL`: Your Neon PostgreSQL connection string.
   - `SERPER_API_KEY`: Your Serper.dev API key.

5. **Run the script:**
   ```bash
   python monitor.py
   ```

## Getting the GitHub Secrets Ready

For this to run automatically, you need to configure three secrets in your GitHub repository:
1. Go to your repository on GitHub.
2. Click **Settings** > **Secrets and variables** > **Actions**.
3. Click "New repository secret" and add:
   - `DATABASE_URL` (Connection string to your Neon database)
   - `SERPER_API_KEY` (Key from Serper.dev)
   - `GEMINI_API_KEY` (Not used in this phase, but preparing for Phase 2)

## How to Add New RSS Feeds

1. Open `monitor.py`.
2. Locate the corresponding fetcher function (e.g., `fetch_reddit()` or `fetch_quora()`).
3. Add the new RSS feed URL to the `urls` list.
4. The system will automatically fetch, clean, and process it on the next run.

## How to Add New Serper Keywords

1. Open `monitor.py`.
2. Locate the `fetch_serper()` function.
3. Add your new search string to the `queries` list.
