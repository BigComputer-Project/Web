from huggingface_hub import snapshot_download

snapshot_download(repo_id="HuggingFaceFW/fineweb-edu", repo_type="dataset", allow_patterns="sample/350BT/01*.parquet", local_dir="/mnt/hdd/benjamin/fineweb/350BT")

