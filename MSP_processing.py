
import lxml.etree as ET

file_path = "MSP/VO_RRMSPSV_0000_9965_20170210_8ba9b6f2-f58b-49e4-827f-152f0b65d32e.xml"

tree = ET.parse(file_path)
root = tree.getroot()

print(root.tag)