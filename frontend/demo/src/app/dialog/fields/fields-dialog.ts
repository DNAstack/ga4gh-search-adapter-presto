import { Component, OnInit } from '@angular/core';
import { MatDialogRef } from '@angular/material';

@Component({
  selector: 'fields-dialog',
  templateUrl: './fields-dialog.html'
})
export class FieldsDialog {

  form: FormGroup;

  constructor(
    private dialogRef: MatDialogRef<FieldsDialog>
  ) {}

  ngOnInit() {
  }
}
