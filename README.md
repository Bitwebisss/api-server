sudo systemctl daemon-reload
sudo systemctl enable --now bitweb-api


sudo nano /etc/nginx/conf.d/ngserver-global.conf


# Global IP blacklist (override in server blocks with another geo if needed)
geo $blocked_ip {
    default 0;
    # 1.2.3.4 1;
}

# Global User-Agent blacklist
map $http_user_agent $blocked_agent {
    default 0;
    ~*(sqlmap|nmap|nikto|burp|scanner|bot|crawler|spider|scraper) 1;
}

# Global malicious query blacklist
map $args $blocked_args {
    default 0;
    ~*(union.*select|drop.*table|insert.*into|\.\./|etc/passwd) 1;
}

nano /etc/systemd/system/bitweb-api.service

[Unit]
Description=Bitweb API Gunicorn
After=network.target

[Service]
User=explorer
Group=explorer
WorkingDirectory=/home/explorer/api-server-master
Environment="PATH=/home/explorer/api-server-master/venv/bin"
ExecStart=/home/explorer/api-server-master/venv/bin/gunicorn --bind 127.0.0.1:21223 --worker-class eventlet -w 1 --timeout 0 app:app
Restart=always

[Install]
WantedBy=multi-user.target

sudo systemctl daemon-reload
sudo systemctl enable --now bitweb-api
sudo systemctl status bitweb-api

sudo journalctl -u bitweb-api -n 50

sudo journalctl -u bitweb-api -f

sudo journalctl -u bitweb-api --since today