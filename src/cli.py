import argparse
from datetime import datetime, timedelta
from nasa_data_retrieval import get_cme_data, get_gst_data, analyze_data, create_visualizations

def parse_date(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError as e:
        raise argparse.ArgumentTypeError(f"Invalid date format. Use YYYY-MM-DD: {e}")

def main():
    parser = argparse.ArgumentParser(description="NASA Space Weather Data Retrieval and Analysis Tool")
    
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=(datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d"),
        help="Start date in YYYY-MM-DD format (default: 30 days ago)"
    )
    
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=datetime.now().strftime("%Y-%m-%d"),
        help="End date in YYYY-MM-DD format (default: today)"
    )
    
    parser.add_argument(
        "--data-type",
        choices=["cme", "gst", "both"],
        default="both",
        help="Type of data to retrieve (default: both)"
    )
    
    parser.add_argument(
        "--output-format",
        choices=["csv", "json"],
        default="csv",
        help="Output format for the data (default: csv)"
    )
    
    parser.add_argument(
        "--visualize",
        action="store_true",
        help="Create visualizations of the data"
    )
    
    args = parser.parse_args()
    
    try:
        if args.data_type in ["cme", "both"]:
            cme_data = get_cme_data(args.start_date, args.end_date)
        
        if args.data_type in ["gst", "both"]:
            gst_data = get_gst_data(args.start_date, args.end_date)
        
        if args.data_type == "both":
            analyze_data(cme_data, gst_data, output_format=args.output_format)
        
        if args.visualize:
            create_visualizations(cme_data if args.data_type == "cme" else gst_data)
            
    except Exception as e:
        parser.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()

