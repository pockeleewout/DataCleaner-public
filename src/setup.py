"""
Setup script for Super Database Cleaner 5000 (SDC5000)
"""

import app
from database import *

try:
    drop_all()
except Exception:
    # Ignore DropTable failing if the table doesn't exist
    db.session.rollback()

# Create all the things !!!
create_all()
