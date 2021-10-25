# Run from GCP Console Cloud to set up Python environments to create DataFlow template file later on

# Make sure this file has the exec permission i.e. run chmod +x first

# Set up SSH certificate - do this manually to link with the GitHub repo on github.com before proceeding.
# mkdir ~/.ssh
# cd ~/.ssh
# ssh-keygen -t ed25519 -C "aaron.altrock@icloud.com"
# cat ~/.ssh/id_rsa.pub
# eval "$(ssh-agent -s)"

# Clone git repo
mkdir /home/aaron_altrock/git
cd /home/aaron_altrock/git/

# Check out Dev branch
# Set up global variables for user profile and email address
git config --global user.email "aaron.altrock@icloud.com"
git config --global user.name "aaltrock"

# Clone git repo
git clone git@github.com:aaltrock/detect_fake_news.git --config core.sshCommand="ssh -i ~/.ssh/id_ed25519"

# Checkout the dev branch
cd /home/aaron_altrock/git/detect_fake_news
git checkout Dev_EDA
git pull

# Make directory for the project
mkdir /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow_bs
cd /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow_bs

# Make new venv and activate
python3 -m venv ./venv
conda deactivate
source ./venv/bin/activate

# Copy over script, config and other files from the Git repo
cp /home/aaron_altrock/git/detect_fake_news/Phase\ 2/Data\ Pipeline/Stage_01_etl/*.* ./




# Run pip to update and install all packages
pip install pip -U
pip install setuptools -U
pip install -r /home/aaron_altrock/git/detect_fake_news/Phase\ 2/Data\ Pipeline/Stage_01_etl/requirements_01_gcp_bucket_to_bigquery_dataflow.txt

