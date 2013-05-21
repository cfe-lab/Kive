from django.contrib import admin;
from django.contrib.contenttypes import generic;
from copperfish.models import *;

admin.site.register(Datatype);

# FIXME refactor for renaming of CodeResource and CodeResourceFamily

class CompoundDatatypeMemberInline(admin.TabularInline):
    model = CompoundDatatypeMember;
    extra = 0;

class CompoundDatatypeAdmin(admin.ModelAdmin):
    inlines = [CompoundDatatypeMemberInline];
    
admin.site.register(CompoundDatatype, CompoundDatatypeAdmin);

class CodeResourceRevisionInline(admin.StackedInline):
    model = CodeResourceRevision;
    extra = 0;

class CodeResourceAdmin(admin.ModelAdmin):
    inlines = [CodeResourceRevisionInline];

class CodeResourceDependencyInline(admin.StackedInline):
    model = CodeResourceDependency;
    fk_name = "coderesourcerevision";
    extra = 0;

class CodeResourceRevisionAdmin(admin.ModelAdmin):
    #exclude = ["MD5_checksum"];
    inlines = [CodeResourceDependencyInline];
    
admin.site.register(CodeResource, CodeResourceAdmin);
admin.site.register(CodeResourceRevision, CodeResourceRevisionAdmin);

class TransformationInputInline(generic.GenericStackedInline):
    model = TransformationInput;
    extra = 0;

class TransformationOutputInline(generic.GenericStackedInline):
    model = TransformationOutput;
    extra = 0;

class MethodAdmin(admin.ModelAdmin):
    inlines = [TransformationInputInline, TransformationOutputInline];

admin.site.register(MethodFamily);
admin.site.register(Method, MethodAdmin);

admin.site.register(TransformationInput);
admin.site.register(TransformationOutput);

class PipelineStepInputInline(admin.StackedInline):
    model = PipelineStepInput;
    extra = 0;

class PipelineStepDeleteInline(admin.StackedInline):
    model = PipelineStepDelete;
    extra = 0;

class PipelineStepAdmin(admin.ModelAdmin):
    inlines = [PipelineStepInputInline, PipelineStepDeleteInline];

class PipelineStepInline(admin.StackedInline):
    model = PipelineStep;
    extra = 0;

class PipelineOutputMappingInline(admin.TabularInline):
    model = PipelineOutputMapping;
    extra = 0;

class PipelineAdmin(admin.ModelAdmin):
    inlines = [TransformationInputInline, PipelineStepInline,
               PipelineOutputMappingInline];

admin.site.register(PipelineStep, PipelineStepAdmin);
admin.site.register(PipelineFamily);
admin.site.register(Pipeline, PipelineAdmin);
admin.site.register(PipelineStepInput);
admin.site.register(PipelineStepDelete);

class DatasetAdmin(admin.ModelAdmin):
    exclude = ["MD5_checksum"];

admin.site.register(Dataset, DatasetAdmin);


# TO DO: figure out if there's a better way to handle
# GenericForeignKeys in admin pages, like we have for
# TransformationInputs/Outputs and for PipelineSteps
