import psutil
import time
import logging


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_node_processes():
    return [p for p in psutil.process_iter(['pid', 'name', 'memory_percent']) if p.info['name'] == 'node']

def kill_excess_processes(processes, threshold):
    if len(processes) <= 1:
        return

    total_memory = sum(p.info['memory_percent'] for p in processes)
    if total_memory <= threshold:
        return

    processes.sort(key=lambda x: x.info['memory_percent'])
    
    for process in processes[1:]:
        try:
            logging.info(f"Attempting to terminate process {process.pid} (memory: {process.info['memory_percent']:.2f}%)")
            process.terminate()
            process.wait(timeout=3)
        except psutil.NoSuchProcess:
            logging.info(f"Process {process.pid} no longer exists")
        except psutil.TimeoutExpired:
            logging.warning(f"Process {process.pid} did not terminate, forcing kill")
            try:
                process.kill()
            except psutil.NoSuchProcess:
                logging.info(f"Process {process.pid} no longer exists after kill attempt")

def monitor_node_processes(threshold=30.0, interval=60):
    while True:
        try:
            node_processes = get_node_processes()
            total_memory = sum(p.info['memory_percent'] for p in node_processes)
            logging.info(f"Total node processes: {len(node_processes)}, Total memory: {total_memory:.2f}%")

            if total_memory > threshold:
                kill_excess_processes(node_processes, threshold)
            
            time.sleep(interval)
        except Exception as e:
            logging.error(f"An error occurred: {str(e)}")
            time.sleep(interval)

if __name__ == "__main__":
    logging.info("Starting node process monitor...")
    monitor_node_processes()