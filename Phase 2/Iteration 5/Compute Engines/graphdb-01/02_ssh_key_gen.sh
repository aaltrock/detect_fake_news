cd ~/.ssh
ssh-keygen -t rsa -b 4096 -N '' -f id_github -C aaron.altrock@icloud.com
mv id_github ~/.ssh
mv id_github.pub ~/.ssh

