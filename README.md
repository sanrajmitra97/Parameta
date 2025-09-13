# Parameta
Takehome Assessment for Parameter Solutions

# Prerequisites
1. Clone this repository using the command `git clone https://github.com/sanrajmitra97/Parameta.git` or any other method you prefer.
2. Enter the folder using `cd Parameta`
3. Ensure you are at the root directory. i.e. If you type `ls` you should see the folders `rates_test` and `stdev_test`. You should see the files `requirements.txt` and this README.
4. Create a new virtual environment by running the command `python -m venv .venv`. On some systems, try it with `python3` instead of `python`.
5. Activate the virtual environment using the command `source .venv/bin/activate` on a Mac/Linux machine. For Windows, use the command `.\.venv\Scripts\activate`. We only require three
packages for this project: `pandas`, `numpy`, `pyarrow`.
6. Install the required packages for this project by running the command `pip install -r requirements.txt`

# Some Notes
On the very first run, there may be a few rare instances where the `main.py` script takes longer than expected to run. On subsequent runs, it should alawys hit < 1s. This is probably caused by some "cold start" factors. On a WSL2 system, it is < 1s on all runs. On a Mac, it takes > 1s on the first run, but all subsequent runs take < 1s.

# Problem 1: Calculating Rates
1. Ensure you are at the root directory. i.e. If you type `ls` you should see the folders `rates_test` and `stdev_test`.
2. Run the command `python ./rates_test/scripts/main.py`
3. Results will be stored in `./rates_test/results/rates_final_data.csv`
4. The column of interest would be `new_price`.
5. If there are any NaN values in `new_price`, it means that the one hour window condition was not met and there is insufficient data.

# Problem 2: Calculating Rolling Standard Deviation
1. Ensure you are at the root directory. i.e. If you type `ls` you should see the folders `rates_test` and `stdev_test`.
2. Run the command `python ./stdev_test/scripts/main.py`
3. Results will be stored in `./stdev_test/results/stdev_test_output.csv`
4. The standard deviations are under the columns <price_type>_stdev where price_type is bid, mid, or ask.