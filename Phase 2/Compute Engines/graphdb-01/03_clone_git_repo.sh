# Set up local git directory
mkdir ~/git
cd ~/git

# Set up global variables for user profile and email address
git config --global user.email "aaron.altrock@icloud.com"
git config --global user.name "aaltrock"


# Clone git repo on Compute Engine
git clone git@github.com:aaltrock/detect_fake_news.git --config core.sshCommand="ssh -i ~/.ssh/id_github"
