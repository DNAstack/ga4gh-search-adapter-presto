import { Component, Inject, OnInit, ViewChild } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';
import { JsonEditorComponent, JsonEditorOptions } from 'ang-jsoneditor';

@Component({
  selector: 'json-dialog',
  templateUrl: './json-dialog.html'
})
export class JsonDialog {

  public editorOptions = new JsonEditorOptions();

  @ViewChild('jsonEditor')
  jsonEditor: JsonEditorComponent;

  public query: any;

  constructor(
    private dialogRef: MatDialogRef<JsonDialog>,
    @Inject(MAT_DIALOG_DATA) public data: any
  ) {
    this.query = data.query;

    this.editorOptions.mode = 'tree';
    this.editorOptions.mainMenuBar = false;
    this.editorOptions.navigationBar = false;
    this.editorOptions.statusBar = false;
  }

  ngOnInit() {
    console.log("Query");
    console.log(this.query);
    console.log("Editor");
    console.log(this.jsonEditor);
    this.jsonEditor.set(JSON.parse(JSON.stringify(this.query)));
  }
}
