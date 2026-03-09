import sys
import os

# Set up path to include src
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '.')))

from src.corporate_actions.reprocessing_worker import ReprocessingWorker
from src.config import logger

def run_worker():
    worker = ReprocessingWorker()
    print("--- Draining Reprocessing Queue ---")
    summary = worker.drain_queue(max_items=200) # Process all pending
    print(f"Summary: {summary['processed']} processed, {summary['succeeded']} succeeded, {summary['failed']} failed.")
    
if __name__ == '__main__':
    run_worker()
