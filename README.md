# NaviCare Crawler

Simple script to crawl medical facility data from Cortico API and other sources, and store it in Supabase database.

## Install dependencies
```bash
pip install -r requirements.txt
```

## Environment Variables

Create a `.env` file with the following variables:
```env
SUPABASE_URL=your_supabase_url
SUPABASE_KEY=your_supabase_key
CORTICO_API_URL=http://cerebro-release.cortico.ca/api/collected-clinics-public/
CRAWLER_BATCH_SIZE=25
CRAWLER_MAX_CONCURRENT=3
CRAWLER_DELAY=1.0
CRAWLER_MAX_RETRIES=3
```

## Usage

### Main Crawler
```bash
# Run full crawl
python main.py --mode full

# Run test crawl (single page)
python main.py --mode test --page 1
```

### Availability Updater
```bash
# Update only availability information (for daily GitHub Actions)
python update_availability.py

# With custom parameters
python update_availability.py --batch-size 50 --delay 0.5
```

### Database Reset
```bash
# Reset database (use with caution)
python reset_database.py
```

## GitHub Actions

The repository includes GitHub Actions workflows for automated data updates:

### 1. Full Data Crawl (cortico-crawl.yml)
- Runs twice daily (7:00 AM and 7:00 PM EDT)
- Performs complete data crawl and update
- Updates all facility information including details, services, hours, etc.
- Cleans up old observation data

### 2. Availability-Only Update (update-availability.yml)
- Runs once daily (between the two full crawls)
- Updates only the availability information for existing facilities
- Lightweight operation that runs faster than full crawl
- Does not clean up old data or modify non-availability fields

To use the GitHub Actions workflows:

1. Set up the following secrets in your repository settings:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase service role key
   - `CORTICO_API_URL`: The Cortico API endpoint (optional, defaults to public endpoint)

2. The workflows will automatically run on their schedules and keep your database updated.