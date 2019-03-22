import { FormsModule, ReactiveFormsModule } from '@angular/forms';
import { NgModule } from '@angular/core';
import { BrowserModule } from '@angular/platform-browser';
import { AppComponent } from './app.component';

import { QueryBuilderModule } from 'angular2-query-builder';

import { NoopAnimationsModule } from '@angular/platform-browser/animations';
import { HttpClientModule } from '@angular/common/http';

import {
  MatToolbarModule,
  MatTooltipModule,
  MatTabsModule,
  MatButtonToggleModule,
  MatSlideToggleModule,
  MatButtonModule,
  MatCheckboxModule,
  MatSelectModule,
  MatInputModule,
  MatDatepickerModule,
  MatNativeDateModule,
  MatRadioModule,
  MatIconModule,
  MatCardModule
} from '@angular/material';
import {NgJsonEditorModule} from 'ang-jsoneditor';

@NgModule({
  imports: [
    NgJsonEditorModule,
    BrowserModule,
    FormsModule,
    ReactiveFormsModule,
    QueryBuilderModule,
    NoopAnimationsModule,
    HttpClientModule,
    MatToolbarModule,
    MatTooltipModule,
    MatTabsModule,
    MatButtonToggleModule,
    MatSlideToggleModule,
    MatButtonModule,
    MatCheckboxModule,
    MatSelectModule,
    MatInputModule,
    MatDatepickerModule,
    MatNativeDateModule,
    MatRadioModule,
    MatIconModule,
    MatCardModule
  ],
  declarations: [ AppComponent ],
  bootstrap: [ AppComponent ]
})
export class AppModule {
}
