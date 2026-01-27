# Email to Database Processor

An automated email processing pipeline that extracts fixed-width customer and vehicle data from Microsoft 365 emails, stores it in PostgreSQL with intelligent upsert logic, and sends back Excel reports as email attachments.

## Features

- **Email Automation**: Fetches unread emails from Microsoft 365 via Graph API
- **Fixed-Width Parser**: Extracts structured customer and vehicle data from email bodies
- **Database Integration**: PostgreSQL storage with SQLAlchemy ORM
- **Upsert Logic**: Intelligent conflict resolution based on customer and vehicle numbers
- **Audit Trail**: Complete ingestion tracking with timestamps and row counts
- **Excel Reports**: Automatic generation and email delivery of processed data
- **Domain Filtering**: Configurable allowed sender domains for security

## Project Structure

```
proc/
├── process_email_graph.py  # Main orchestration script
├── models.py              # SQLAlchemy database models
├── db.py                  # Database connection and session management
├── utils_email.py         # Microsoft Graph API email utilities
├── utils_parser.py        # Fixed-width data parsing logic
├── utils_excel.py         # Excel file generation
├── utils_data.py          # Data sanitization and helpers
├── requirements.txt       # Python dependencies
└── logs/                  # Application logs directory
```

## Prerequisites

- Python 3.8+
- PostgreSQL database
- Microsoft 365 account with Graph API access
- Azure AD app registration with appropriate permissions

## Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/email-to-database-processor.git
cd email-to-database-processor
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file in the project root:
```env
# Database Configuration
DB_USER=your_db_user
DB_PASSWORD=your_db_password
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DATABASE_SCHEMA=your_schema
CUSTOMER_CAR_TABLE=customer_car_data
AUDIT_TABLE=ingestion_audit

# Microsoft Graph API
GRAPH_TENANT_ID=your_tenant_id
GRAPH_CLIENT_ID=your_client_id
GRAPH_CLIENT_SECRET=your_client_secret
IMAP_USER=your_email@domain.com

# Email Settings
ORIGINATOR=sender@domain.com
RECIPIENT=recipient@domain.com
ALLOWED_DOMAINS=trusted-domain1.com,trusted-domain2.com
```

4. Set up the database:
```bash
# Create the required tables using SQLAlchemy
python -c "from models import Base; from db import engine; Base.metadata.create_all(engine)"
```

## Usage

Run the main processing script:
```bash
python process_email_graph.py
```

The script will:
1. Fetch all unread emails from the configured mailbox
2. Filter emails from allowed domains
3. Parse fixed-width data from email bodies
4. Insert/update records in PostgreSQL
5. Generate Excel reports
6. Send reports back via email
7. Mark processed emails as read

## Data Fields

The system processes the following customer and vehicle data:

| Field | Type | Description |
|-------|------|-------------|
| Customer Number | Integer | Unique customer identifier (PK) |
| Customer Full Name | String(100) | Full name of customer |
| Telephone Number | String(20) | Contact phone number |
| E-mail Address | String(100) | Customer email |
| Enquiry Creation Date | Date | When enquiry was created |
| Enquiry Number | Integer | Enquiry reference number |
| Deposit Date | Date | Payment deposit date |
| Invoice Date | Date | Invoice generation date |
| Vehicle Number | Integer | Unique vehicle identifier (PK) |
| Model | String(50) | Vehicle model |
| Variant | String(50) | Model variant |
| Colour Code | String(50) | Vehicle color code |
| Registration Number | String(50) | License plate number |

**Composite Primary Key**: `(customer_number, vehicle_number)`

## Database Schema

### CustomerCarData Table
- Composite primary key on `customer_number` and `vehicle_number`
- Upsert logic updates existing records on conflict
- Stored in configurable schema

### IngestionAudit Table
- Tracks each email processing event
- Records: email ID, timestamps, row counts, status, errors
- Useful for monitoring and debugging

## Configuration

### Field Widths
Fixed-width field parsing is configured in `process_email_graph.py`. Adjust `FIELD_WIDTHS` dictionary if your data format changes.

### Logging
Logs are written to `logs/mail_processor.log` with INFO level by default. Modify logging configuration in `process_email_graph.py` as needed.

## Dependencies

Key dependencies include:
- `SQLAlchemy` - Database ORM
- `psycopg2-binary` - PostgreSQL adapter
- `msal` - Microsoft Authentication Library
- `requests` - HTTP library for Graph API
- `openpyxl` - Excel file generation
- `python-dotenv` - Environment variable management

See [requirements.txt](requirements.txt) for complete list.

## Security Considerations

- Store all credentials in `.env` file (never commit to git)
- Use Azure AD app with minimum required permissions
- Configure `ALLOWED_DOMAINS` to restrict email sources
- Review logs regularly for unauthorized access attempts
- Use PostgreSQL connection pooling for production deployments

## Error Handling

- Empty emails or emails without data rows are skipped
- Failed database operations are logged
- Graph API authentication errors halt processing
- All audit records include error fields for troubleshooting

## Contributing

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

For issues, questions, or contributions, please open an issue on GitHub.

## Acknowledgments

- Built with [SQLAlchemy](https://www.sqlalchemy.org/)
- Email integration via [Microsoft Graph API](https://docs.microsoft.com/en-us/graph/)
- Excel generation using [openpyxl](https://openpyxl.readthedocs.io/)
