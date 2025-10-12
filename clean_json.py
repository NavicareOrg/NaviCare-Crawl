import json

# 读取输入文件
with open('output.json', 'r', encoding='utf-8') as file:
    data = json.load(file)

# 过滤数据
filtered_data = []
for clinic in data:
    # 检查除了name和detail_url外的字段
    fields_to_check = ['phone', 'city', 'province', 'longitude', 'latitude']
    
    # 如果所有检查的字段都是N/A，则跳过
    if all(clinic.get(field) == "N/A" for field in fields_to_check):
        continue
    
    filtered_data.append(clinic)

# 保存到输出文件
with open('ratemd.json', 'w', encoding='utf-8') as file:
    json.dump(filtered_data, file, indent=2, ensure_ascii=False)

print(f"处理完成！原始数据: {len(data)} 条，过滤后: {len(filtered_data)} 条")