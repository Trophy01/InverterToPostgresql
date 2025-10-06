# Battery Monitor Web Application

A secure web application for monitoring battery data with authentication, HTTPS support, and domain configuration.

## Features

- **Secure Authentication**: Battery ID-based login system with password management
- **HTTPS Support**: SSL/TLS encryption with Let's Encrypt certificates
- **Domain Configuration**: Easy setup for custom domains
- **Real-time Monitoring**: Live battery data visualization
- **Responsive Design**: Works on desktop and mobile devices
- **Session Management**: Secure user sessions with Flask-Login

## Quick Start

### Development Setup

For local development:

```bash
cd /home/trophy/BatteryMonitoring2/web
./setup_dev.sh
source venv/bin/activate
python app.py
```

The application will be available at `http://localhost:5000`

### Production Deployment

For production deployment with HTTPS and domain support:

```bash
cd /home/trophy/BatteryMonitoring2/web
./deploy.sh
```

Follow the prompts to enter your domain name and email address.

## Authentication System

### How It Works

1. **First Login**: Users enter their battery ID and create a password
2. **Account Creation**: The system automatically creates a user account
3. **Subsequent Logins**: Users use their battery ID and password
4. **Password Management**: Users can change their passwords anytime

### Database Schema

The authentication system uses a `users` table with the following structure:

```sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    battery_id VARCHAR(50) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    first_login BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    last_login TIMESTAMPTZ,
    is_active BOOLEAN DEFAULT TRUE
);
```

## Configuration

### Environment Variables

The application uses the following environment variables:

- `DB_HOST`: Database host (default: 127.0.0.1)
- `DB_PORT`: Database port (default: 5432)
- `DB_NAME`: Database name (default: batteries)
- `DB_USER`: Database user (default: troy)
- `DB_PASS`: Database password (default: s3rv3r5mx)
- `SECRET_KEY`: Flask secret key for sessions

### SSL Certificates

For production, SSL certificates are automatically obtained from Let's Encrypt. The certificates are stored in:
- `/etc/letsencrypt/live/your-domain.com/fullchain.pem`
- `/etc/letsencrypt/live/your-domain.com/privkey.pem`

## File Structure

```
web/
├── app.py                 # Main Flask application
├── requirements.txt       # Python dependencies
├── nginx.conf            # Nginx configuration
├── battery-monitor.service # Systemd service file
├── deploy.sh             # Production deployment script
├── setup_dev.sh          # Development setup script
├── templates/
│   ├── index.html        # Main dashboard
│   ├── login.html        # Login page
│   └── change_password.html # Password change page
└── static/
    ├── style.css         # Stylesheet
    └── main.js           # JavaScript
```

## API Endpoints

All API endpoints require authentication:

- `GET /` - Main dashboard
- `GET /login` - Login page
- `POST /login` - Process login
- `GET /logout` - Logout user
- `GET /change-password` - Password change page
- `POST /change-password` - Process password change
- `GET /api/devices` - List available devices
- `GET /api/summary/<device_id>` - Get device summary
- `GET /api/series/<device_id>` - Get time series data

## Security Features

- **Password Hashing**: Uses bcrypt for secure password storage
- **Session Management**: Secure session handling with Flask-Login
- **HTTPS Enforcement**: All traffic redirected to HTTPS
- **Security Headers**: HSTS, X-Frame-Options, XSS protection
- **Input Validation**: SQL injection protection with parameterized queries

## Monitoring and Logs

### Service Management

```bash
# Check service status
sudo systemctl status battery-monitor

# View logs
sudo journalctl -u battery-monitor -f

# Restart service
sudo systemctl restart battery-monitor
```

### Nginx Logs

- Access logs: `/var/log/nginx/battery-monitor-access.log`
- Error logs: `/var/log/nginx/battery-monitor-error.log`

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify PostgreSQL is running
   - Check database credentials in environment variables
   - Ensure the `users` table exists

2. **SSL Certificate Issues**
   - Check domain DNS settings
   - Verify nginx configuration
   - Run `sudo certbot renew` to renew certificates

3. **Service Won't Start**
   - Check logs: `sudo journalctl -u battery-monitor -f`
   - Verify file permissions
   - Check virtual environment setup

### Manual Database Setup

If the automatic setup fails, manually create the users table:

```bash
psql -h 127.0.0.1 -p 5432 -U troy -d batteries -f ../db/create_users_table.sql
```

## Development

### Adding New Features

1. Update the Flask application in `app.py`
2. Add new templates in `templates/`
3. Update static files in `static/`
4. Test locally with `python app.py`
5. Deploy with `./deploy.sh`

### Dependencies

Key dependencies:
- Flask 3.0.3 - Web framework
- Flask-Login 0.6.3 - Authentication
- bcrypt 4.1.2 - Password hashing
- psycopg2 - PostgreSQL adapter
- gunicorn 21.2.0 - WSGI server

## Support

For issues or questions:
1. Check the logs for error messages
2. Verify all configuration files
3. Ensure all dependencies are installed
4. Check database connectivity