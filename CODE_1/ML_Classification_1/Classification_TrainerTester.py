##########################################################################################################################################
# Import Required Modules
##########################################################################################################################################

import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, Dataset
import pandas as pd
import numpy as np
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
import itertools

##########################################################################################################################################
# Dataset Generator for Training and Testing 
##########################################################################################################################################

# Define a PyTorch Dataset class
class CoarseLabelDataset(Dataset):
    def __init__(self, data_path):
        self.data = pd.read_csv(data_path)
        self.features = self.data.drop(columns=['timestamp', 'coarse_label']).values.astype(np.float32)
        self.labels = self.data['coarse_label'].values.astype(np.int64)

    def __len__(self):
        return len(self.data)

    def __getitem__(self, idx):
        return self.features[idx], self.labels[idx]

##########################################################################################################################################
# Model/Training/Evaluation 
##########################################################################################################################################

# Define the neural network
class CoarseLabelClassifier(nn.Module):
    def __init__(self, input_size, num_classes):
        super(CoarseLabelClassifier, self).__init__()
        self.fc = nn.Sequential(
            nn.Linear(input_size, 10),
            nn.ReLU(),
            nn.Linear(10, 5),
            nn.ReLU(),
            nn.Linear(5, num_classes)
        )

    def forward(self, x):
        return self.fc(x)

# Train the model
def train_model(model, train_loader, criterion, optimizer, device):
    model.train()
    total_loss = 0
    for features, labels in train_loader:
        features, labels = features.to(device), labels.to(device)
        optimizer.zero_grad()
        outputs = model(features)
        loss = criterion(outputs, labels)
        loss.backward()
        optimizer.step()
        total_loss += loss.item()
    return total_loss / len(train_loader)

# Evaluate the model
def evaluate_model(model, test_loader, device):
    model.eval()
    all_preds = []
    all_labels = []
    with torch.no_grad():
        for features, labels in test_loader:
            features, labels = features.to(device), labels.to(device)
            outputs = model(features)
            _, preds = torch.max(outputs, 1)
            all_preds.extend(preds.cpu().numpy())
            all_labels.extend(labels.cpu().numpy())
    return np.array(all_labels), np.array(all_preds)

##########################################################################################################################################
# Plotting and Computing Results
##########################################################################################################################################

# Plot confusion matrix
def plot_confusion_matrix(labels, preds, class_names, title="Confusion Matrix"):
    """
    Plot a confusion matrix using matplotlib with annotations.

    Args:
        cm (numpy.ndarray): Confusion matrix (2D array).
        class_names (list): List of class names corresponding to labels.
        title (str): Title for the confusion matrix plot.
    """

    cm = confusion_matrix(labels, preds)

    plt.figure(figsize=(10, 8))
    plt.imshow(cm, interpolation='nearest', cmap='Blues')  # Use 'Blues' colormap
    plt.title(title)
    plt.colorbar()

    # Add class names to axes
    tick_marks = np.arange(len(class_names))
    plt.xticks(tick_marks, class_names, rotation=45, ha="right")
    plt.yticks(tick_marks, class_names)

    # Add annotations to each cell
    for i, j in itertools.product(range(cm.shape[0]), range(cm.shape[1])):
        plt.text(j, i, f"{cm[i, j]}", horizontalalignment="center",
                 color="white" if cm[i, j] > cm.max() / 2 else "black")

    plt.tight_layout()
    plt.ylabel('True Label')
    plt.xlabel('Predicted Label')
    plt.show()

# Computing Results from Confusion Matrix
def calculate_tfpn_percentages(labels, preds, class_names, output_csv="tfpn_percentages.csv"):
    """
    Calculate percentages of True Positives (TP), False Positives (FP), True Negatives (TN), and False Negatives (FN)
    for each class based on the confusion matrix, and save the results as a CSV.

    Args:
        labels (numpy.ndarray): True labels.
        preds (numpy.ndarray): Predicted labels.
        class_names (list): List of class names corresponding to labels.
        output_csv (str): File path to save the CSV.

    Returns:
        pandas.DataFrame: DataFrame with percentages of TP, FP, TN, FN for each class.
    """
    cm = confusion_matrix(labels, preds)
    num_classes = len(class_names)
    total_samples = cm.sum()

    results = {
        "Class": [],
        "True Positives (%)": [],
        "False Positives (%)": [],
        "True Negatives (%)": [],
        "False Negatives (%)": []
    }

    for i in range(num_classes):
        TP = cm[i, i]
        FP = cm[:, i].sum() - TP
        FN = cm[i, :].sum() - TP
        TN = total_samples - (TP + FP + FN)

        TP_percent = (TP / total_samples) * 100
        FP_percent = (FP / total_samples) * 100
        TN_percent = (TN / total_samples) * 100
        FN_percent = (FN / total_samples) * 100

        results["Class"].append(class_names[i])
        results["True Positives (%)"].append(TP_percent)
        results["False Positives (%)"].append(FP_percent)
        results["True Negatives (%)"].append(TN_percent)
        results["False Negatives (%)"].append(FN_percent)

    # Convert results to a DataFrame
    df_results = pd.DataFrame(results)

    # Save the DataFrame to a CSV file
    df_results.to_csv(output_csv, index=False)
    print(f"Results saved to {output_csv}")

    return df_results

##########################################################################################################################################
# Main Script
##########################################################################################################################################

# File paths
train_csv = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\Final_TrainData.csv"
test_csv = "C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\Final_TestData.csv"

# Hyperparameters
batch_size = 100
num_epochs = 10000
learning_rate = 0.001

# Load datasets
train_dataset = CoarseLabelDataset(train_csv)
test_dataset = CoarseLabelDataset(test_csv)

train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

# Model, loss, and optimizer
input_size = train_dataset.features.shape[1]
num_classes = len(np.unique(train_dataset.labels))
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

model = CoarseLabelClassifier(input_size, num_classes).to(device)
criterion = nn.CrossEntropyLoss()
optimizer = optim.Adam(model.parameters(), lr=learning_rate)

# Training loop
train_losses = []
for epoch in range(num_epochs):
    train_loss = train_model(model, train_loader, criterion, optimizer, device)
    train_losses.append(train_loss)
    print(f"Epoch {epoch+1}/{num_epochs}, Loss: {train_loss:.4f}")

train_loader_1 = DataLoader(train_dataset, batch_size=1, shuffle=False)
test_loader_1 = DataLoader(test_dataset, batch_size=1, shuffle=False)

# Evaluate the model
labels_train, preds_train, = evaluate_model(model, train_loader_1, device)
labels_test, preds_test = evaluate_model(model, test_loader_1, device)

# Plot learning curve
plt.plot(range(1, num_epochs + 1), train_losses, marker='o')
plt.xlabel('Epoch')
plt.ylabel('Loss')
plt.title('Training Loss Curve')
plt.show()

# Plot confusion matrix
class_names = ["Null", "Still", "Walking", "Run", "Bike", "Car", "Bus", "Train", "Subway"]
plot_confusion_matrix(labels_train, preds_train, class_names, title="Confusion Matrix - Training")
plot_confusion_matrix(labels_test, preds_test, class_names, title="Confusion Matrix - Testing")

# Compute True/False Positive/Negative Percentages
calculate_tfpn_percentages(labels_train, preds_train, class_names, output_csv="C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\tfpn_percentages_Train.csv")
calculate_tfpn_percentages(labels_test, preds_test, class_names, output_csv="C:\\Users\\ninad\\OneDrive - Washington State University (email.wsu.edu)\\24_CPTS415_TeamJam\\Data\\TrainingTesting\\tfpn_percentages_Test.csv")

