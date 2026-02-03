import json

with open('absl_api_test.json', 'r') as f:
    data = json.load(f)

print(f"ReturnCode: {data.get('ReturnCode')}")
print(f"Total items: {len(data.get('AccordionList', []))}")

nov_items = [x for x in data.get('AccordionList', []) 
             if 'November' in x.get('ResourceLink', '') 
             and '2024' in x.get('ResourceLink', '')]

print(f"November 2024 items: {len(nov_items)}")
if nov_items:
    print(f"Sample title: {nov_items[0].get('ResourceLink')}")
    print(f"Sample URL: {nov_items[0].get('pdfUrl')}")
