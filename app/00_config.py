import json
import os
from os import path
from dotenv import load_dotenv
from pathlib import Path

# Load local environmental variables
load_dotenv()

def main():

	src_base_dir = os.environ.get('SRC_BASE_DIR')

	if str(path.exists(f"{src_base_dir}/config.json")) == 'False':
		stocks = [  
            {"url":"https://www.moneycontrol.com/us-markets/stockpricequote/nvidia/NVDA"},    
            {"url":"https://www.moneycontrol.com/us-markets/stockpricequote/advancedmicrodevices/AMD"},
            {"url":"https://www.moneycontrol.com/us-markets/stockpricequote/microsoft/MSFT"}
        ]
		myJSON = json.dumps(stocks)
		with open(f"{src_base_dir}/config.json", "w") as jsonfile: jsonfile.write(myJSON) 
		print("Configuration file generated")
	else:
		print("Existing configuration file available")
	
if __name__== "__main__":
	main()