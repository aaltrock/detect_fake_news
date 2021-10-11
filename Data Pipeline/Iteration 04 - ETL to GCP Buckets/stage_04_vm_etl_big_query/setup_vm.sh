# Bash shell script to set up a VM on GCP

# Set up GUI - Gnome Desktop based on
# https://cloud.google.com/architecture/chrome-desktop-remote-on-compute-engine#gnome
sudo apt update
sudo apt install --assume-yes wget tasksel
[[ $(/usr/bin/lsb_release --codename --short) == "stretch" ]] &&    sudo apt install --assume-yes libgbm1/stretch-backports
wget https://dl.google.com/linux/direct/chrome-remote-desktop_current_amd64.deb
sudo apt-get install --assume-yes ./chrome-remote-desktop_current_amd64.deb
sudo DEBIAN_FRONTEND=noninteractive
sudo DEBIAN_FRONTEND=noninteractive     apt install --assume-yes  task-gnome-desktop
sudo bash -c 'echo "exec /etc/X11/Xsession /usr/bin/gnome-session" > /etc/chrome-remote-desktop-session'
sudo systemctl disable lightdm.service
wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
sudo apt install --assume-yes ./google-chrome-stable_current_amd64.deb
DISPLAY= /opt/google/chrome-remote-desktop/start-host --code="4/0AX4XfWjfCxE1xwe9totJ91qcUhKhc0p0h5iOSf3n_ty1Xu-phGjMCRYzBDVivnHtO-Aw7w" --redirect-url="https://remotedesktop.google.com/_/oauthredirect" --name=$(hostname)
sudo systemctl status chrome-remote-desktop@$USER

# Set up Python
mkdir -p /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow
sudo apt-get install build-essential
sudo apt-get install -y python3-venv
sudo apt-get install git
cd /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow
python3 -m venv ./venv

# Specific to Python script execution - 01_clean_csv_local_to_bucket.py
mkdir -p /home/aaron_altrock/PycharmProjects/detect_fake_news_data_flow/json_src


