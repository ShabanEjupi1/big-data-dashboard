#!/bin/bash
# Quick script to add freeze mode to remaining pages

echo "Adding freeze mode to recommendations.html and portfolio.html..."

# This will be done through Python file editing
python3 << 'EOF'
import re

# Function to add freeze mode CSS
def add_freeze_css(filename):
    with open(filename, 'r') as f:
        content = f.read()
    
    # Check if already has freeze mode
    if 'btn-freeze' in content:
        print(f"✓ {filename} already has freeze mode")
        return
    
    # Find .nav-back:hover and add freeze CSS after it
    freeze_css = '''
        .btn-freeze {
            background: #06b6d4;
            border: 2px solid #0891b2;
            cursor: pointer;
            font-weight: bold;
        }
        
        .btn-freeze:hover {
            background: #0891b2;
        }
        
        .btn-freeze.frozen {
            background: #dc2626;
            animation: pulse-freeze 2s infinite;
        }
        
        @keyframes pulse-freeze {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.7; }
        }
        
        .frozen-badge {
            display: none;
            background: #dc2626;
            color: white;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 0.9em;
            font-weight: bold;
            animation: pulse-freeze 2s infinite;
        }
        
        .frozen-badge.active {
            display: inline-block;
        }'''
    
    # Add after .nav-back:hover
    content = re.sub(
        r'(\.nav-back:hover\s*\{[^}]+\})',
        r'\1' + freeze_css,
        content
    )
    
    with open(filename, 'w') as f:
        f.write(content)
    
    print(f"✓ Added freeze CSS to {filename}")

# Add to both files
add_freeze_css('/home/krenuser/big-data-dashboard/templates/recommendations.html')
add_freeze_css('/home/krenuser/big-data-dashboard/templates/portfolio.html')

print("\n✓ Freeze mode CSS added to both files")
EOF

echo "Done!"
