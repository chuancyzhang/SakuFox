import sys

with open('web/dashboard.html', 'r', encoding='utf-8') as f:
    content = f.read()

target = '''      <div class="logo-area">
        <i class="fa-solid fa-fox fa-lg logo-icon" style="color: #fca5a5;"></i>
        <h1 style="letter-spacing: 0.5px;">SakuFox ??</h1>
        <span class="badge" style="background: linear-gradient(135deg, #fecaca, #fca5a5); border: none; color: #7f1d1d;">v1.0</span>
      </div>
      <div class="user-area" style="display: flex; align-items: center; gap: 12px;">'''

replacement = '''      <div class="logo-area">
        <i class="fa-solid fa-fox fa-lg logo-icon" style="color: #fca5a5;"></i>
        <h1 style="letter-spacing: 0.5px;" data-i18n="nav_brand">SakuFox ??</h1>
        <span class="badge" style="background: linear-gradient(135deg, #fecaca, #fca5a5); border: none; color: #7f1d1d;">v1.0</span>
      </div>
      <div class="nav-menu" style="flex: 1; display: flex; align-items: center; margin-left: 32px; gap: 24px;">
        <a href="/dashboard" class="nav-link active" data-i18n="nav_analysis" style="color: #2b3a4a; text-decoration: none; font-weight: 600; font-size: 15px; border-bottom: 2px solid #ef4444; padding-bottom: 4px;">数据分析</a>
        <a href="/web/knowledge.html" class="nav-link" data-i18n="nav_knowledge" style="color: #64748b; text-decoration: none; font-weight: 500; font-size: 15px; padding-bottom: 4px; transition: color 0.2s;" onmouseover="this.style.color='#2b3a4a'" onmouseout="this.style.color='#64748b'">知识库配置</a>
      </div>
      <div class="user-area" style="display: flex; align-items: center; gap: 12px;">'''

if target in content:
    content = content.replace(target, replacement)
    with open('web/dashboard.html', 'w', encoding='utf-8') as f:
        f.write(content)
    print("Success")
else:
    print("Target not found. Doing fallback replace...")
    # Maybe try replacing without whitespace concern
    import re
    # Match the inner HTML
    target_pattern = re.compile(r'<div class="logo-area">.*?<h1 style="letter-spacing: 0.5px;">SakuFox ??</h1>.*?</div>\s*<div class="user-area"', re.DOTALL)
    if target_pattern.search(content):
        # We know exactly what to put
        new_content = re.sub(target_pattern, replacement.replace('      <div class="user-area"', '<div class="user-area"'), content)
        with open('web/dashboard.html', 'w', encoding='utf-8') as f:
            f.write(new_content)
        print("Fallback Replace Success")
    else:
        print("Fallback Target not found either.")

