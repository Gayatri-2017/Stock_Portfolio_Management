import os
from PIL import Image
# rootdir = '/home/abhishek/project-bigdata2/cluster_analysis_3_4/with_sentiment/cluster_df_cl3/cluster3_k_means_with_senti_viz'
rootdir = '/Users/apple/Desktop/BD-2/Project/Our_gitlab_1/Code/UI/static/Portfolio_Generation'

for subdir, dirs, files in os.walk(rootdir):
    for file in files:
         file_name = os.path.join(subdir, file)
         if (file_name.endswith("png")):
            print(file_name)
            im = Image.open(file_name)
            rgb_im = im.convert('RGB')
            new_name = file_name[:-3]+str("jpg")
            print(new_name)
            rgb_im.save(new_name)