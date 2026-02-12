from hiring_compass_au.data.pipelines.job_alerts.run import run_job_alert_pipeline
import logging


def main():
    logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

    run_job_alert_pipeline()
    
if __name__ == "__main__":
    main()