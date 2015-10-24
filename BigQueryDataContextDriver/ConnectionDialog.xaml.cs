using System.Windows;
using LINQPad.Extensibility.DataContext;

namespace BigQuery.Linq
{
    public partial class ConnectionDialog : Window
    {
        readonly IConnectionInfo connectionInfo;
        readonly DriverProperty property;

        public ConnectionDialog(IConnectionInfo connectionInfo)
        {
            InitializeComponent();
            this.connectionInfo = connectionInfo;
            this.property = new DriverProperty(connectionInfo);

            JsonTextBox.Text = property.ContextJsonAuthenticationKey;
            UserTextBox.Text = property.ContextUser;
            ProjectIdBox.Text = property.ContextProjectId;
            UseDataSetCheckBox.IsChecked = property.ContextIsOnlyDataSet;
            DataSetBox.Text = property.ContextDataSet;
        }

        private void OK_Click(object sender, RoutedEventArgs e)
        {
            property.ContextJsonAuthenticationKey = JsonTextBox.Text;
            property.ContextUser = UserTextBox.Text;
            property.ContextProjectId = ProjectIdBox.Text;
            property.ContextIsOnlyDataSet = UseDataSetCheckBox.IsChecked ?? false;
            property.ContextDataSet = DataSetBox.Text;

            property.DisplayName = property.ContextProjectId + (property.ContextIsOnlyDataSet ? $"({property.ContextDataSet})" : "");

            this.DialogResult = true;
        }

        private void Cancel_Click(object sender, RoutedEventArgs e)
        {
            this.DialogResult = false;
        }
    }
}