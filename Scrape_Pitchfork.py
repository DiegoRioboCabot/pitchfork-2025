import time
start = time.time()

import subprocess

subprocess.run(["python", "Scrape_Pitchfork_Sitemap.py"], check=True)
subprocess.run(["python", "Scrape_Pitchfork_Unreachable_URLS.py"], check=True)

end = time.time()
print(f"\nExecution Time: {(end - start)/60:.2f} minutes")