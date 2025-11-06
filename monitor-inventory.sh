#!/bin/bash

# ==========================================
# KartShoppe Inventory Monitor
# Monitors productId, name, category, and inventory
# ==========================================

API_URL="http://localhost:8081/api/ecommerce/inventory/state"
INTERVAL=3

while true; do
    clear
    echo "========================================="
    echo "📦 Inventory Dashboard - $(date)"
    echo "========================================="

    # Fetch and format products, then sort by productId
    curl -s "$API_URL" | jq -r '
        .products[] |
        [.productId, .name, .category, (.inventory | tostring)] |
        join("|")
    ' | sort -t '|' -k1,1 | awk -F'|' '
        BEGIN {
            printf "%-12s %-50s %-20s %-10s\n", "ProductID", "Name", "Category", "Inventory";
            print "-------------------------------------------------------------------------------------------"
        }
        {
            printf "%-12s %-50s %-20s %-10d\n", $1, $2, $3, $4
        }
    '

    echo ""
    echo "Refreshing every ${INTERVAL}s... (Ctrl+C to exit)"
    sleep $INTERVAL
done