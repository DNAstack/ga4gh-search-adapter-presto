import {BrowserModule} from '@angular/platform-browser';
import {APP_INITIALIZER, NgModule} from '@angular/core';

import {AppRoutingModule} from './app-routing.module';
import {AppComponent} from './app.component';
import {NgJsonEditorModule} from 'ang-jsoneditor';
import {FormsModule, ReactiveFormsModule} from '@angular/forms';
import {QueryBuilderModule} from 'angular2-query-builder';
import {BrowserAnimationsModule, NoopAnimationsModule} from '@angular/platform-browser/animations';
import {HttpClientModule} from '@angular/common/http';

import {
  MatButtonModule,
  MatButtonToggleModule,
  MatCardModule,
  MatCheckboxModule,
  MatDatepickerModule,
  MatDialogModule,
  MatIconModule,
  MatInputModule,
  MatNativeDateModule,
  MatRadioModule,
  MatSelectModule,
  MatSlideToggleModule,
  MatSnackBarModule,
  MatTableModule,
  MatTabsModule,
  MatToolbarModule,
  MatTooltipModule,
  MatProgressSpinnerModule,
  MatPaginatorModule,
  MatSidenavModule,
  MatExpansionModule,
  MatProgressBarModule,
  MatBadgeModule,
  MatChipsModule
} from '@angular/material';
import {AppConfigService} from './app-config.service';
import { JsonDialog } from './dialog/json/json-dialog';
import { FieldsDialogComponent } from './dialog/fields/fields-dialog.component';
import { ClipboardModule } from 'ngx-clipboard';

@NgModule({
  declarations: [
    AppComponent,
    JsonDialog,
    FieldsDialogComponent
  ],
  entryComponents: [
    JsonDialog,
    FieldsDialogComponent
  ],
  imports: [
    BrowserModule,
    AppRoutingModule,
    NgJsonEditorModule,
    BrowserAnimationsModule,
    BrowserModule,
    FormsModule,
    ReactiveFormsModule,
    QueryBuilderModule,
    HttpClientModule,
    MatToolbarModule,
    MatTooltipModule,
    MatTableModule,
    MatTabsModule,
    MatButtonToggleModule,
    MatSlideToggleModule,
    MatButtonModule,
    MatCheckboxModule,
    MatSelectModule,
    MatInputModule,
    MatDatepickerModule,
    MatDialogModule,
    MatNativeDateModule,
    MatRadioModule,
    MatIconModule,
    MatCardModule,
    MatSnackBarModule,
    MatProgressSpinnerModule,
    MatPaginatorModule,
    MatSidenavModule,
    MatExpansionModule,
    MatProgressBarModule,
    MatBadgeModule,
    MatChipsModule,
    ClipboardModule
  ],
  providers: [
    AppConfigService,
    {
      provide: APP_INITIALIZER,
      useFactory: (appConfig: AppConfigService) => {
        return () => {
          return appConfig.loadAppConfig();
        };
      },
      multi: true,
      deps: [AppConfigService]
    }
  ],
  bootstrap: [AppComponent]
})
export class AppModule {
}
