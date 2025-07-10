import requests
import time
import logging
from datetime import datetime
from typing import Dict, Any

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('logs/circuit_breaker.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def create_todo() -> Dict[str, Any]:
    """Create a new todo item."""
    try:
        logger.info("Attempting to create new todo item...")
        response = requests.post(
            'https://jsonplaceholder.typicode.com/posts',
            json={
                'title': 'foo',
                'body': 'bar',
                'userId': 1
            },
            headers={
                'Content-type': 'application/json; charset=UTF-8'
            }
        )
        response.raise_for_status()
        result = response.json()
        logger.info(f"Successfully created todo with ID: {result['id']}")
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to create todo: {str(e)}")
        raise

def get_todo(todo_id: int) -> Dict[str, Any]:
    """Get todo item details."""
    try:
        logger.info(f"Fetching todo details for ID: {todo_id}")
        response = requests.get(f'https://jsonplaceholder.typicode.com/posts/{todo_id}')
        response.raise_for_status()
        result = response.json()
        logger.info(f"Successfully retrieved todo details: {result}")
        return result
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch todo {todo_id}: {str(e)}")
        raise

def delete_todo(todo_id: int) -> bool:
    """Delete a todo item."""
    try:
        logger.info(f"Attempting to delete todo with ID: {todo_id}")
        response = requests.delete(f'https://jsonplaceholder.typicode.com/posts/{todo_id}')
        response.raise_for_status()
        logger.info(f"Successfully deleted todo {todo_id}")
        return True
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to delete todo {todo_id}: {str(e)}")
        raise

def circuit_breaker_operation() -> Dict[str, Any]:
    """Main circuit breaker operation."""
    start_time = datetime.now()
    logger.info("Starting circuit breaker operation")
    
    try:
        # Step 1: Create todo
        logger.info("STEP 1: Creating todo")
        todo = create_todo()
        todo_id = todo['id']
        
        # Step 2: Poll for todo details 5 times
        logger.info("STEP 2: Starting polling sequence")
        for i in range(5):
            logger.info(f"Poll attempt {i + 1} of 5")
            todo_details = get_todo(todo_id)
            
            if i < 4:  # Don't sleep on the last iteration
                logger.info(f"Waiting 10 seconds before next poll...")
                time.sleep(10)
        
        # Step 3: Delete todo
        logger.info("STEP 3: Deleting todo")
        success = delete_todo(todo_id)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        result = {
            "todo_id": todo_id,
            "status": "completed",
            "polls_completed": 5,
            "deletion_success": success,
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat()
        }
        
        logger.info(f"Circuit breaker operation completed successfully: {result}")
        return result
        
    except Exception as e:
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        error_result = {
            "status": "error",
            "error_message": str(e),
            "duration_seconds": duration,
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat()
        }
        
        logger.error(f"Circuit breaker operation failed: {error_result}")
        return error_result

if __name__ == "__main__":
    try:
        result = circuit_breaker_operation()
        print(f"Operation completed with result: {result}")
    except Exception as e:
        print(f"Operation failed with error: {e}")
        exit(1) 