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

### Page Range Crawler
```bash
# Run crawl for specific page range
python crawl_page_range.py --start-page 1 --end-page 10

# Run crawl for specific lab page range
python crawl_lab_page_range.py --start-page 1 --end-page 10

# Run crawl for specific pharmacy page range
python crawl_pharmacy_page_range.py --start-page 1 --end-page 10

# With custom parameters
python crawl_page_range.py --start-page 1 --end-page 10 --batch-size 50 --delay 0.5
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
- Runs weekly on Sunday at 03:00 UTC
- Performs complete data crawl and update
- Updates all facility information including details, services, hours, etc.
- Cleans up old observation data

### 2. Availability-Only Update (update-availability.yml)
- Runs daily at 17:00 UTC
- Updates only the availability information for existing facilities
- Lightweight operation that runs faster than full crawl
- Does not clean up old data or modify non-availability fields

### 3. Segmented Crawl (segmented-crawl.yml)
- Runs automatically from Sunday to Wednesday at 03:00 UTC
- Each day processes a different segment (50 pages per segment)
- Segment 1: Sunday (pages 1-50)
- Segment 2: Monday (pages 51-100)
- Segment 3: Tuesday (pages 101-150)
- Segment 4: Wednesday (pages 151-200)
- Prevents GitHub Actions 6-hour timeout by breaking work into smaller chunks

### 4. Segmented Lab Crawl (segmented-lab-crawl.yml)
- Runs automatically from Sunday to Wednesday at 03:00 UTC
- Each day processes a different segment (50 pages per segment)
- Segment 1: Sunday (pages 1-50)
- Segment 2: Monday (pages 51-100)
- Segment 3: Tuesday (pages 101-150)
- Segment 4: Wednesday (pages 151-200)
- Prevents GitHub Actions 6-hour timeout by breaking work into smaller chunks

### 5. Segmented Pharmacy Crawl (segmented-pharmacy-crawl.yml)
- Runs automatically from Sunday to Wednesday at 03:00 UTC
- Each day processes a different segment (50 pages per segment)
- Segment 1: Sunday (pages 1-50)
- Segment 2: Monday (pages 51-100)
- Segment 3: Tuesday (pages 101-150)
- Segment 4: Wednesday (pages 151-200)
- Prevents GitHub Actions 6-hour timeout by breaking work into smaller chunks

### 6. Segment Coordinator (segment-coordinator.yml)
- Runs Thursday at 03:00 UTC after all segments complete
- Performs coordination tasks after all segments finish
- Can include database consistency checks, reporting, etc.

To use the GitHub Actions workflows:

1. Set up the following secrets in your repository settings:
   - `SUPABASE_URL`: Your Supabase project URL
   - `SUPABASE_KEY`: Your Supabase service role key
   - `CORTICO_API_URL`: The Cortico API endpoint (optional, defaults to public endpoint)

2. The workflows will automatically run on their schedules and keep your database updated.