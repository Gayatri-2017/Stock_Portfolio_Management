import pandas as pd
cols = ["PC_0", "PC_1", "PC_2", "PC_3", "PC_4", "PC_5", "PC_6", "PC_7", "symbol", "sector", "label", "Quarter"]
complete_df = pd.DataFrame(columns=cols)
for i in range(1, 9):
	file_name = str(i) + "_pca_qt_result.csv"
	print("file_name = ", file_name)
	
	df = pd.read_csv(file_name)
	df["Quarter"] = i
	complete_df = complete_df.append(df, ignore_index = True, sort=False)

complete_df = complete_df[cols]
print("complete_df = \n", complete_df)
complete_df.to_csv("pca_complete_result.csv")