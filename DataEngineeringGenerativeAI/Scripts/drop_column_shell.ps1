# PowerShell script to drop the 'Loud Cover' column from weatherHistory.csv and save as weatherHistoryShell.csv

# Import the CSV file
$data = Import-Csv "..\Data\weatherHistory.csv"

# Drop the 'Loud Cover' column by selecting all properties except it
$data | Select-Object * -ExcludeProperty "Loud Cover" | Export-Csv "..\Data\weatherHistoryShell.csv" -NoTypeInformation

# Optional: Display a message
Write-Host "Column 'Loud Cover' dropped and file saved as weatherHistoryShell.csv"
