import { Component, Inject, OnInit, ViewChild } from '@angular/core';
import { MAT_DIALOG_DATA, MatDialogRef } from '@angular/material';
import { JsonEditorComponent, JsonEditorOptions } from 'ang-jsoneditor';
import { ClipboardModule } from 'ngx-clipboard';

@Component({
  selector: 'json-dialog',
  templateUrl: './json-dialog.html',
  styleUrls: ['./json-dialog.scss'],
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

    this.editorOptions.mode = 'view';
    this.editorOptions.mainMenuBar = false;
    this.editorOptions.navigationBar = false;
    this.editorOptions.statusBar = false;    
    this.editorOptions.expandAll = true;
  }

  ngOnInit() {
    
  }
}
