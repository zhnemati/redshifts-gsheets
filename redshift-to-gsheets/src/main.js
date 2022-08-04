
// Sets up a UI pop up that takes the query from user and passes it to main_execution_function

function onOpen() {
    SpreadsheetApp.getUi() 
        .createMenu('Custom Menu')
        .addItem('Show prompt', 'showPrompt')
        .addToUi();
  }
  
  function showPrompt() {
    var ui = SpreadsheetApp.getUi(); 
  
    var result = ui.prompt(
        'Its query time !',
        'Please enter your SQL query:',
        ui.ButtonSet.OK_CANCEL);
    Utilities.sleep(10000) //Wait for 10 seconds
    // Process the user's response.
    var button = result.getSelectedButton();
    var query = result.getResponseText();
    if (button == ui.Button.OK) {
      // User clicked "OK".
      ui.alert('Starting processing for' + query + '.');
      main_execution_procedure(query)
    } else if (button == ui.Button.CANCEL) {
      // User clicked "Cancel".
      ui.alert('Failed to read your entered query.');
    } else if (button == ui.Button.CLOSE) {
      // User clicked X in the title bar.
      ui.alert('You closed the dialog.');
    }
  }
  
  