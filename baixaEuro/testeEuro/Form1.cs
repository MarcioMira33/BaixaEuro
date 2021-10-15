using baixaEuro;
using System;
using System.Windows.Forms;

namespace testeEuro
{
    public partial class Form1 : Form
    {
        public Form1()
        {
            InitializeComponent();
        }

        private void Form1_Load(object sender, EventArgs e)
        {
           string stringCon = "Data Source=VENEZA.UNIEURO.INT;Initial Catalog=BDSPFP;User ID=User_GrpCeuma;Password=G*pC&#m@08.19?";
           Class_Teste teste = new Class_Teste();
           //teste.AtivarBaixaService();
           //teste.nfeInternetCaixa(stringCon);
          teste.buscaNaoBaixados(stringCon, "2021-10-08");
            //teste.baixarBoletos2();  
            //Service1 serv = new Service1();
            // serv.AtivarBaixaService();
            MessageBox.Show("terminou");
        }
    }
}